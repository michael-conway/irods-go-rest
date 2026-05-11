package httpapi

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

func TestOpenAPIContractMatchesRegisteredRoutes(t *testing.T) {
	openapiOps, err := parseOpenAPIOperations(filepath.Join("..", "..", "api", "openapi.yaml"))
	if err != nil {
		t.Fatalf("parse openapi operations: %v", err)
	}

	runtimeOps, err := parseRegisteredRouteOperations("handler.go")
	if err != nil {
		t.Fatalf("parse runtime routes: %v", err)
	}

	var missingInRuntime []string
	for op := range openapiOps {
		if _, ok := runtimeOps[op]; !ok {
			missingInRuntime = append(missingInRuntime, op)
		}
	}

	var undocumentedInOpenAPI []string
	for op := range runtimeOps {
		if !openapiOperationsFilter(op) {
			continue
		}
		if _, ok := openapiOps[op]; !ok {
			undocumentedInOpenAPI = append(undocumentedInOpenAPI, op)
		}
	}

	sort.Strings(missingInRuntime)
	sort.Strings(undocumentedInOpenAPI)

	if len(missingInRuntime) > 0 || len(undocumentedInOpenAPI) > 0 {
		t.Fatalf("openapi/runtime route mismatch\nmissing in runtime: %v\nundocumented in openapi: %v", missingInRuntime, undocumentedInOpenAPI)
	}
}

func parseOpenAPIOperations(path string) (map[string]struct{}, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	ops := make(map[string]struct{})
	var currentPath string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "  /") && strings.HasSuffix(strings.TrimSpace(line), ":") {
			currentPath = strings.TrimSpace(strings.TrimSuffix(line, ":"))
			continue
		}

		trimmed := strings.TrimSpace(line)
		switch trimmed {
		case "get:", "post:", "put:", "patch:", "delete:", "head:":
			if currentPath == "" {
				continue
			}
			method := strings.ToUpper(strings.TrimSuffix(trimmed, ":"))
			op := fmt.Sprintf("%s %s", method, currentPath)
			if openapiOperationsFilter(op) {
				ops[op] = struct{}{}
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return ops, nil
}

func parseRegisteredRouteOperations(path string) (map[string]struct{}, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	routeRE := regexp.MustCompile(`mux\.Handle(?:Func)?\("([A-Z]+) ([^"]+)"`)
	ops := make(map[string]struct{})

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		matches := routeRE.FindStringSubmatch(line)
		if len(matches) != 3 {
			continue
		}
		op := fmt.Sprintf("%s %s", matches[1], matches[2])
		if openapiOperationsFilter(op) {
			ops[op] = struct{}{}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return ops, nil
}

func openapiOperationsFilter(op string) bool {
	parts := strings.SplitN(op, " ", 2)
	if len(parts) != 2 {
		return false
	}
	path := parts[1]
	return path == "/healthz" || strings.HasPrefix(path, "/api/v1/")
}
