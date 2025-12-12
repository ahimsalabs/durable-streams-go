// Command snippet extracts marked code snippets from source files and outputs them as markdown.
//
// Usage:
//
//	snippet [flags] <file>...
//	  -name string    Extract specific snippet (default: all)
//	  -lang string    Override language detection
//	  -format string  Output format: simple|mdx (default: mdx)
//	  -out string     Output file (default: stdout)
//	  -inject string  Inject snippets into this file (e.g., README.md)
//	  -root string    Root directory for scoping snippet names (e.g., dir:name)
//
// Snippet markers in source files:
//
//	// [snippet:example-name]
//
//	func Example() {
//	    // code here
//	}
//	// [/snippet:example-name]
//
// README injection markers:
//
//	<!-- [snippet:example-name] -->
//	<!-- [/snippet:example-name] -->
//
// Whole-file snippets (no markers needed in source):
//
//	<!-- [snippet-file:path/to/file.ts] -->
//
// Lint mode (-lint) checks for large code blocks not covered by snippets.
//
// Scoped snippets:
//
// When -root is specified, snippet names are scoped by directory path relative to root.
// For example, with -root /repo and file /repo/examples/newsserver/main.go:
//
//	Source marker:  // [snippet:handlers]
//	Scoped name:    examples/newsserver:handlers
//	README marker:  <!-- [snippet:examples/newsserver:handlers] -->
package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

var (
	nameFlag     = flag.String("name", "", "Extract specific snippet (default: all)")
	langFlag     = flag.String("lang", "", "Override language detection")
	formatFlag   = flag.String("format", "mdx", "Output format: simple|mdx")
	outFlag      = flag.String("out", "", "Output file (default: stdout)")
	injectFlag   = flag.String("inject", "", "Inject snippets into this file (e.g., README.md)")
	rootFlag     = flag.String("root", "", "Root directory for scoping snippet names (e.g., dir:name)")
	lintFlag     = flag.Bool("lint", false, "Check for large code blocks not covered by snippets")
	lintMinLines = flag.Int("lint-min-lines", 5, "Minimum lines for lint warnings (default: 5)")
)

// Snippet represents an extracted code snippet.
type Snippet struct {
	Name      string
	File      string
	StartLine int
	EndLine   int
	Content   string
	Lang      string
	Collapse  bool // If true, collapse function bodies to "// ..."
}

var (
	// Matches: // [snippet:name] or # [snippet:name]
	startPattern = regexp.MustCompile(`(?://|#)\s*\[snippet:([^\]]+)\]`)
	// Matches: // [/snippet:name] or # [/snippet:name]
	endPattern = regexp.MustCompile(`(?://|#)\s*\[/snippet:([^\]]+)\]`)

	// README markers: <!-- [snippet:name] --> and <!-- [/snippet:name] -->
	readmeStartPattern = regexp.MustCompile(`<!--\s*\[snippet:([^\]]+)\]\s*-->`)
	readmeEndPattern   = regexp.MustCompile(`<!--\s*\[/snippet:([^\]]+)\]\s*-->`)

	// Whole-file snippet: <!-- [snippet-file:path/to/file.ts] -->
	fileSnippetPattern = regexp.MustCompile(`<!--\s*\[snippet-file:([^\]]+)\]\s*-->`)

	// Lint ignore marker: <!-- snippet-ignore --> (block-level)
	ignorePattern = regexp.MustCompile(`<!--\s*snippet-ignore\s*-->`)

	// Lint disable marker: <!-- snippet-lint-disable --> (file-level)
	lintDisablePattern = regexp.MustCompile(`<!--\s*snippet-lint-disable\s*-->`)

	// Code block detection for linting
	codeBlockStartPattern = regexp.MustCompile("^```")
	codeBlockEndPattern   = regexp.MustCompile("^```$")
)

func main() {
	flag.Parse()

	// Lint mode: check README for unmanaged code blocks
	if *lintFlag {
		if flag.NArg() == 0 {
			fmt.Fprintln(os.Stderr, "Usage: snippet -lint <readme-file>")
			os.Exit(1)
		}
		issues := lintReadme(flag.Arg(0), *lintMinLines)
		if len(issues) > 0 {
			for _, issue := range issues {
				fmt.Fprintln(os.Stderr, issue)
			}
			os.Exit(1)
		}
		return
	}

	if flag.NArg() == 0 {
		fmt.Fprintln(os.Stderr, "Usage: snippet [flags] <file>...")
		flag.PrintDefaults()
		os.Exit(1)
	}

	var allSnippets []Snippet

	// Resolve root directory for scoping
	root := *rootFlag
	if root != "" {
		var err error
		root, err = filepath.Abs(root)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error resolving root path: %v\n", err)
			os.Exit(1)
		}
	}

	for _, file := range flag.Args() {
		snippets, err := extractSnippets(file, root)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error processing %s: %v\n", file, err)
			os.Exit(1)
		}
		allSnippets = append(allSnippets, snippets...)
	}

	// Filter by name if specified
	if *nameFlag != "" {
		var filtered []Snippet
		for _, s := range allSnippets {
			if s.Name == *nameFlag {
				filtered = append(filtered, s)
			}
		}
		allSnippets = filtered
	}

	// Inject mode: update a README file with snippets
	if *injectFlag != "" {
		if err := injectSnippets(*injectFlag, allSnippets, *formatFlag, root); err != nil {
			fmt.Fprintf(os.Stderr, "Error injecting snippets: %v\n", err)
			os.Exit(1)
		}
		return
	}

	// Generate output
	output := formatSnippets(allSnippets, *formatFlag)

	// Write output
	if *outFlag != "" {
		if err := os.WriteFile(*outFlag, []byte(output), 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "Error writing output: %v\n", err)
			os.Exit(1)
		}
	} else {
		fmt.Print(output)
	}
}

func extractSnippets(filename string, root string) ([]Snippet, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Compute scope prefix from file path relative to root
	scopePrefix := ""
	if root != "" {
		absFile, err := filepath.Abs(filename)
		if err == nil {
			if rel, err := filepath.Rel(root, absFile); err == nil {
				// Use directory path as scope (e.g., "examples/newsserver")
				dir := filepath.Dir(rel)
				if dir != "." {
					scopePrefix = filepath.ToSlash(dir) + ":"
				}
			}
		}
	}

	var snippets []Snippet
	var current *Snippet
	var contentLines []string

	scanner := bufio.NewScanner(file)
	lineNum := 0

	for scanner.Scan() {
		lineNum++
		line := scanner.Text()

		// Check for start marker
		if matches := startPattern.FindStringSubmatch(line); matches != nil {
			if current != nil {
				return nil, fmt.Errorf("line %d: nested snippet %q inside %q", lineNum, matches[1], current.Name)
			}
			// Parse name and attributes: "name attr1 attr2"
			name, attrs := parseSnippetMarker(matches[1])
			current = &Snippet{
				Name:      scopePrefix + name, // Scoped name: "dir:name"
				File:      filepath.Base(filename),
				StartLine: lineNum + 1, // Content starts on next line
				Lang:      detectLang(filename),
				Collapse:  attrs["collapse"],
			}
			contentLines = nil
			continue
		}

		// Check for end marker
		if matches := endPattern.FindStringSubmatch(line); matches != nil {
			if current == nil {
				return nil, fmt.Errorf("line %d: end marker for %q without start", lineNum, matches[1])
			}
			// Compare against unscoped name (source files use simple names)
			// End markers don't have attributes, just the name
			endName, _ := parseSnippetMarker(matches[1])
			expectedEnd := scopePrefix + endName
			if expectedEnd != current.Name {
				return nil, fmt.Errorf("line %d: end marker %q doesn't match start %q", lineNum, endName, current.Name)
			}
			current.EndLine = lineNum - 1
			current.Content = strings.Join(contentLines, "\n")
			// Trim leading blank line if present (from blank line after marker)
			current.Content = strings.TrimPrefix(current.Content, "\n")
			// Collapse function bodies if requested (before dedent to preserve indentation detection)
			if current.Collapse {
				current.Content = collapseBodies(current.Content, current.Lang)
			}
			// Dedent: remove common leading whitespace
			current.Content = dedent(current.Content)
			snippets = append(snippets, *current)
			current = nil
			continue
		}

		// Collect content if inside a snippet
		if current != nil {
			contentLines = append(contentLines, line)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	if current != nil {
		return nil, fmt.Errorf("unclosed snippet %q", current.Name)
	}

	return snippets, nil
}

// dedent removes common leading whitespace from all lines.
func dedent(s string) string {
	lines := strings.Split(s, "\n")
	if len(lines) == 0 {
		return s
	}

	// Find minimum indentation (ignoring empty lines)
	minIndent := -1
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		indent := len(line) - len(strings.TrimLeft(line, " \t"))
		if minIndent < 0 || indent < minIndent {
			minIndent = indent
		}
	}

	if minIndent <= 0 {
		return s
	}

	// Remove the common indentation
	for i, line := range lines {
		if len(line) >= minIndent {
			lines[i] = line[minIndent:]
		}
	}

	return strings.Join(lines, "\n")
}

// parseSnippetMarker parses "name attr1 attr2" into name and attributes.
// Returns the name and a map of boolean attributes (presence = true).
func parseSnippetMarker(s string) (name string, attrs map[string]bool) {
	parts := strings.Fields(s)
	if len(parts) == 0 {
		return "", nil
	}
	name = parts[0]
	attrs = make(map[string]bool)
	for _, attr := range parts[1:] {
		attrs[attr] = true
	}
	return name, attrs
}

// collapseBodies collapses indented content to "// ..." comments.
// Uses the first non-empty line to determine base indent, then collapses
// any lines that are more indented. Language-agnostic.
func collapseBodies(content string, lang string) string {
	lines := strings.Split(content, "\n")
	if len(lines) == 0 {
		return content
	}

	// Find base indent from first non-empty line
	baseIndent := -1
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		baseIndent = len(line) - len(strings.TrimLeft(line, " \t"))
		break
	}
	if baseIndent < 0 {
		return content
	}

	// Choose comment style based on language
	commentPrefix := "// "
	if lang == "python" {
		commentPrefix = "# "
	}

	var result []string
	var collapsedIndent string // indent of the collapsed block
	inCollapsed := false

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)

		// Empty lines: keep if not in collapsed block
		if trimmed == "" {
			if !inCollapsed {
				result = append(result, line)
			}
			continue
		}

		indent := len(line) - len(strings.TrimLeft(line, " \t"))

		if indent > baseIndent {
			// More indented than base - collapse it
			if !inCollapsed {
				// Starting a new collapsed block
				collapsedIndent = line[:indent]
				result = append(result, collapsedIndent+commentPrefix+"...")
				inCollapsed = true
			}
			// Skip this line (it's collapsed)
			continue
		}

		// At base indent - keep this line
		inCollapsed = false
		result = append(result, line)
	}

	return strings.Join(result, "\n")
}

func detectLang(filename string) string {
	if *langFlag != "" {
		return *langFlag
	}

	ext := strings.ToLower(filepath.Ext(filename))
	switch ext {
	case ".go":
		return "go"
	case ".ts":
		return "typescript"
	case ".tsx":
		return "tsx"
	case ".js":
		return "javascript"
	case ".jsx":
		return "jsx"
	case ".proto":
		return "protobuf"
	case ".yaml", ".yml":
		return "yaml"
	case ".json":
		return "json"
	case ".md":
		return "markdown"
	case ".sh", ".bash":
		return "bash"
	default:
		return ""
	}
}

func formatSnippet(s Snippet, format string) string {
	var sb strings.Builder

	switch format {
	case "simple":
		sb.WriteString(fmt.Sprintf("```%s\n", s.Lang))
		sb.WriteString(s.Content)
		sb.WriteString("\n```")

	case "mdx":
		sb.WriteString(fmt.Sprintf("```%s title=\"%s\"\n", s.Lang, s.File))
		sb.WriteString(s.Content)
		sb.WriteString("\n```")

	default:
		sb.WriteString(fmt.Sprintf("```%s\n", s.Lang))
		sb.WriteString(s.Content)
		sb.WriteString("\n```")
	}

	return sb.String()
}

func formatSnippets(snippets []Snippet, format string) string {
	var sb strings.Builder

	for i, s := range snippets {
		if i > 0 {
			sb.WriteString("\n\n")
		}
		sb.WriteString(formatSnippet(s, format))
	}

	if len(snippets) > 0 {
		sb.WriteString("\n")
	}

	return sb.String()
}

// lookupSnippet finds a snippet by name with hierarchical fallback.
// For "examples/newsserver:list-params", it first tries exact match,
// then searches for any snippet named "list-params" under "examples/newsserver/**".
func lookupSnippet(snippetMap map[string]Snippet, name string) (Snippet, bool) {
	// Try exact match first
	if s, ok := snippetMap[name]; ok {
		return s, true
	}

	// Parse into scope and snippet name
	idx := strings.LastIndex(name, ":")
	if idx == -1 {
		return Snippet{}, false
	}
	requestedScope := name[:idx]
	snippetName := name[idx+1:]

	// Search for any snippet under the requested scope with matching name
	for fullName, s := range snippetMap {
		idx := strings.LastIndex(fullName, ":")
		if idx == -1 {
			continue
		}
		scope := fullName[:idx]
		sName := fullName[idx+1:]

		// Check if scope is under requestedScope and name matches
		if sName == snippetName && strings.HasPrefix(scope, requestedScope) {
			return s, true
		}
	}

	return Snippet{}, false
}

func injectSnippets(filename string, snippets []Snippet, format string, root string) error {
	// Build a map of snippets by name
	snippetMap := make(map[string]Snippet)
	for _, s := range snippets {
		snippetMap[s.Name] = s
	}

	var missingSnippets []string

	// Resolve base directory for file snippets (relative to the README's directory)
	baseDir := filepath.Dir(filename)
	if root != "" {
		baseDir = root
	}

	// Read the file
	content, err := os.ReadFile(filename)
	if err != nil {
		return err
	}

	lines := strings.Split(string(content), "\n")
	var result []string
	var currentSnippet string
	var skipping bool

	var skipUntilCodeBlockEnd bool // For snippet-file: skip existing code block

	for i, line := range lines {
		// Skip existing code block content after snippet-file marker
		if skipUntilCodeBlockEnd {
			if codeBlockEndPattern.MatchString(line) {
				skipUntilCodeBlockEnd = false
			}
			continue
		}

		// Check for whole-file snippet marker
		if matches := fileSnippetPattern.FindStringSubmatch(line); matches != nil {
			filePath := matches[1]
			// Resolve relative to base directory
			if !filepath.IsAbs(filePath) {
				filePath = filepath.Join(baseDir, filePath)
			}

			fileContent, err := os.ReadFile(filePath)
			if err != nil {
				return fmt.Errorf("line %d: cannot read file %q: %v", i+1, matches[1], err)
			}

			// Create a synthetic snippet for the file
			s := Snippet{
				Name:    matches[1],
				File:    filepath.Base(filePath),
				Content: strings.TrimSuffix(string(fileContent), "\n"),
				Lang:    detectLang(filePath),
			}
			result = append(result, line)
			result = append(result, formatSnippet(s, format))

			// Check if next non-empty line starts a code block - if so, skip it
			for j := i + 1; j < len(lines); j++ {
				nextLine := strings.TrimSpace(lines[j])
				if nextLine == "" {
					continue
				}
				if codeBlockStartPattern.MatchString(lines[j]) {
					skipUntilCodeBlockEnd = true
				}
				break
			}
			continue
		}

		// Check for start marker
		if matches := readmeStartPattern.FindStringSubmatch(line); matches != nil {
			currentSnippet = matches[1]
			result = append(result, line)

			// Insert the snippet content
			if s, ok := lookupSnippet(snippetMap, currentSnippet); ok {
				result = append(result, formatSnippet(s, format))
			} else {
				missingSnippets = append(missingSnippets, currentSnippet)
			}
			skipping = true
			continue
		}

		// Check for end marker
		if matches := readmeEndPattern.FindStringSubmatch(line); matches != nil {
			if matches[1] != currentSnippet {
				return fmt.Errorf("mismatched README markers: expected %q, got %q", currentSnippet, matches[1])
			}
			result = append(result, line)
			skipping = false
			currentSnippet = ""
			continue
		}

		// Skip old content between markers
		if skipping {
			continue
		}

		result = append(result, line)
	}

	if skipping {
		return fmt.Errorf("unclosed README snippet marker %q", currentSnippet)
	}

	if len(missingSnippets) > 0 {
		return fmt.Errorf("missing snippets: %v", missingSnippets)
	}

	// Write back
	return os.WriteFile(filename, []byte(strings.Join(result, "\n")), 0o644)
}

// lintReadme checks for large code blocks that aren't managed by snippets.
func lintReadme(filename string, minLines int) []string {
	content, err := os.ReadFile(filename)
	if err != nil {
		return []string{fmt.Sprintf("Error reading %s: %v", filename, err)}
	}

	lines := strings.Split(string(content), "\n")
	var issues []string

	var inSnippet bool     // Inside <!-- [snippet:...] --> markers
	var inCodeBlock bool   // Inside ``` markers
	var codeBlockStart int // Line number where code block started
	var codeBlockLines int // Number of lines in current code block
	var codeBlockLang string
	var ignoreNext bool // <!-- snippet-ignore --> seen before code block

	for i, line := range lines {
		lineNum := i + 1

		// Check for file-level disable
		if lintDisablePattern.MatchString(line) {
			return nil // Skip entire file
		}

		// Check for ignore marker
		if ignorePattern.MatchString(line) {
			ignoreNext = true
			continue
		}

		// Track snippet regions
		if readmeStartPattern.MatchString(line) || fileSnippetPattern.MatchString(line) {
			inSnippet = true
			continue
		}
		if readmeEndPattern.MatchString(line) {
			inSnippet = false
			continue
		}

		// Track code blocks
		if !inCodeBlock && codeBlockStartPattern.MatchString(line) {
			inCodeBlock = true
			codeBlockStart = lineNum
			codeBlockLines = 0
			// Extract language from ```lang
			codeBlockLang = strings.TrimPrefix(line, "```")
			codeBlockLang = strings.TrimSpace(strings.Split(codeBlockLang, " ")[0])
			continue
		}

		if inCodeBlock {
			if codeBlockEndPattern.MatchString(line) {
				// Code block ended - check if it needs a snippet
				if !inSnippet && !ignoreNext && codeBlockLines >= minLines && isCodeLang(codeBlockLang) {
					issues = append(issues, fmt.Sprintf(
						"%s:%d: %d-line %s code block not covered by snippet",
						filename, codeBlockStart, codeBlockLines, langOrDefault(codeBlockLang),
					))
				}
				inCodeBlock = false
				codeBlockLines = 0
				ignoreNext = false
			} else {
				codeBlockLines++
			}
		}
	}

	return issues
}

// isCodeLang returns true if the language is a programming language (not bash, text, etc.)
func isCodeLang(lang string) bool {
	switch strings.ToLower(lang) {
	case "go", "typescript", "ts", "javascript", "js", "tsx", "jsx", "proto", "protobuf":
		return true
	default:
		return false
	}
}

func langOrDefault(lang string) string {
	if lang == "" {
		return "unknown"
	}
	return lang
}
