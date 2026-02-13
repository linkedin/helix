# Helix Development Guidelines

## Design Documents

When creating or modifying design documents for this repository:

1. **Use the template**: All design docs MUST follow the structure in `docs/design/000-TEMPLATE.md`.
2. **Location**: Place design docs in `docs/design/` with sequential numbering: `NNN-short-title.md`.
3. **Required sections**: Every design doc must include at minimum: Summary, Problem Statement, Goals/Non-Goals, Design, Implementation Plan (with file paths and validation commands), and Testing Strategy.
4. **Status tracking**: Always set the Status field in the metadata table (Draft / In Review / Approved / Implementing / Done).
5. **Module references**: List affected modules (helix-core, helix-rest, etc.) in the metadata table.
6. **Implementation steps**: Each step must reference specific file paths relative to the repo root, the module it belongs to, and a validation command (e.g., `mvn test -pl helix-core -Dtest=TestClassName`).
7. **Diagrams**: Use Mermaid syntax for architecture, sequence, and state diagrams. Use ASCII art only when Mermaid cannot express the concept.
8. **Cross-references**: Link to related design docs using relative paths: `[Title](./NNN-title.md)`.

## Build and Test

- Full build: `mvn clean install`
- Build without tests: `mvn clean install -Dmaven.test.skip.exec=true`
- Run specific module tests: `mvn test -pl helix-core`
- Run specific test class: `mvn test -pl helix-core -Dtest=TestClassName`
- Integration tests: `mvn verify -pl helix-core -P integration-test`
- Frontend: `cd helix-front && yarn install && yarn test`
