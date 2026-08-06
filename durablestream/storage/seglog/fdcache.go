package seglog

import (
	"container/list"
	"errors"
	"os"
	"path/filepath"
	"sync"
)

// fdCache is a bounded descriptor LRU. A pin keeps its descriptor open; cold
// entries are reopened by path. The cache never calls back into stream state,
// so its mutex is always a leaf in the lock order.
type fdCache struct {
	mu       sync.Mutex
	limit    int
	entries  map[string]*fdEntry
	lru      list.List
	closed   bool
	deferred map[string]struct{}
}

type fdEntry struct {
	path string
	f    *os.File
	pins int
	elem *list.Element
}

type fdPin struct {
	c *fdCache
	e *fdEntry
}

func newFDCache(limit int) *fdCache {
	return &fdCache{limit: limit, entries: make(map[string]*fdEntry), deferred: make(map[string]struct{})}
}

func (c *fdCache) pin(path string, _ bool) (*fdPin, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, os.ErrClosed
	}
	if e := c.entries[path]; e != nil {
		e.pins++
		c.lru.MoveToFront(e.elem)
		return &fdPin{c: c, e: e}, nil
	}

	// Stream files are owned by this store and opened read-write so a cold
	// read cannot leave an active file cached with insufficient write access.
	flags := os.O_RDWR
	f, err := os.OpenFile(path, flags, 0)
	if err != nil {
		return nil, err
	}
	e := &fdEntry{path: path, f: f, pins: 1}
	e.elem = c.lru.PushFront(e)
	c.entries[path] = e
	c.evict()
	return &fdPin{c: c, e: e}, nil
}

func (p *fdPin) file() *os.File { return p.e.f }

func (p *fdPin) release() error {
	if p == nil || p.c == nil {
		return nil
	}
	c, e := p.c, p.e
	p.c = nil
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries[e.path] != e {
		return nil
	}
	e.pins--
	var err error
	if e.pins == 0 {
		if _, remove := c.deferred[e.path]; remove {
			delete(c.deferred, e.path)
			c.removeEntry(e)
			err = errors.Join(e.f.Close(), removeFile(e.path), syncDir(filepath.Dir(e.path)))
		} else if c.closed {
			c.removeEntry(e)
			err = e.f.Close()
		}
	}
	c.evict()
	return err
}

func (c *fdCache) evict() {
	for len(c.entries) > c.limit {
		var victim *fdEntry
		for elem := c.lru.Back(); elem != nil; elem = elem.Prev() {
			e := elem.Value.(*fdEntry)
			if e.pins == 0 {
				victim = e
				break
			}
		}
		if victim == nil {
			return
		}
		c.removeEntry(victim)
		_ = victim.f.Close()
	}
}

func (c *fdCache) removeEntry(e *fdEntry) {
	delete(c.entries, e.path)
	c.lru.Remove(e.elem)
}

// unlink removes immediately when cold and otherwise defers removal until the
// final pin is released. This preserves checkpoint-before-unlink ordering
// without invalidating copied reads already in progress.
func (c *fdCache) unlink(path string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e := c.entries[path]; e != nil {
		if e.pins != 0 {
			c.deferred[path] = struct{}{}
			return nil
		}
		c.removeEntry(e)
		if err := e.f.Close(); err != nil {
			return err
		}
	}
	return removeFile(path)
}

func removeFile(path string) error {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (c *fdCache) close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	var errs []error
	for _, e := range c.entries {
		c.removeEntry(e)
		errs = append(errs, e.f.Close())
		if _, remove := c.deferred[e.path]; remove {
			delete(c.deferred, e.path)
			errs = append(errs, removeFile(e.path), syncDir(filepath.Dir(e.path)))
		}
	}
	return errors.Join(errs...)
}
