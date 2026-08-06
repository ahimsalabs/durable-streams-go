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
	deferred map[string]func(int64)
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
	return &fdCache{limit: limit, entries: make(map[string]*fdEntry), deferred: make(map[string]func(int64))}
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
		if onRemoved, remove := c.deferred[e.path]; remove {
			delete(c.deferred, e.path)
			c.removeEntry(e)
			bytes, removeErr := removeOpenFile(e)
			if removeErr == nil && bytes > 0 && onRemoved != nil {
				onRemoved(bytes)
			}
			err = errors.Join(removeErr, syncDir(filepath.Dir(e.path)))
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
	return c.unlinkNotify(path, nil)
}

// unlinkNotify invokes onRemoved with the logical file size after the path is
// physically removed. If a reader has the file pinned, removal and the
// callback are deferred until the final pin is released.
func (c *fdCache) unlinkNotify(path string, onRemoved func(int64)) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e := c.entries[path]; e != nil {
		if e.pins != 0 {
			if _, exists := c.deferred[path]; !exists || onRemoved != nil {
				c.deferred[path] = onRemoved
			}
			return nil
		}
		c.removeEntry(e)
		bytes, err := removeOpenFile(e)
		if err == nil && bytes > 0 && onRemoved != nil {
			onRemoved(bytes)
		}
		return err
	}
	bytes, err := removeFileAndSize(path)
	if err == nil && bytes > 0 && onRemoved != nil {
		onRemoved(bytes)
	}
	return err
}

func removeFileAndSize(path string) (int64, error) {
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if err := os.Remove(path); err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	return info.Size(), nil
}

func removeOpenFile(e *fdEntry) (int64, error) {
	info, statErr := e.f.Stat()
	closeErr := e.f.Close()
	removeErr := os.Remove(e.path)
	if os.IsNotExist(removeErr) {
		removeErr = nil
	}
	if statErr != nil || closeErr != nil || removeErr != nil {
		return 0, errors.Join(statErr, closeErr, removeErr)
	}
	return info.Size(), nil
}

func (c *fdCache) close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	var errs []error
	for _, e := range c.entries {
		c.removeEntry(e)
		errs = append(errs, e.f.Close())
		if onRemoved, remove := c.deferred[e.path]; remove {
			delete(c.deferred, e.path)
			bytes, removeErr := removeFileAndSize(e.path)
			if removeErr == nil && bytes > 0 && onRemoved != nil {
				onRemoved(bytes)
			}
			errs = append(errs, removeErr, syncDir(filepath.Dir(e.path)))
		}
	}
	return errors.Join(errs...)
}
