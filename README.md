# slatr

**slatr** (Scala Translator) - Efficient XML to modern format converter with distributed processing support

## Overview

slatr is a high-performance Scala-based tool for converting potentially large XML files to modern formats like JSON, JSONL, Avro, and Parquet. It's designed for efficient processing of big data workloads with Apache Spark.

### Key Features

- ✅ **Streaming XML Parser** - Memory-efficient processing of large files
- ✅ **XSD Auto-Download & Caching** - Automatically fetches and caches XSD schemas from XML headers
- ✅ **Smart Schema Inference** - Hybrid approach using XSD + sampling + manual overrides
- ✅ **Array Handling** - Correctly handles single-item vs multi-item arrays
- ✅ **Multiple Output Formats** - JSON, JSONL (Avro and Parquet coming soon)
- ✅ **CLI Tool** - User-friendly command-line interface
- 🚧 **Spark Integration** - Distributed processing support (coming in future release)
- 🚧 **BigQuery Integration** - Direct streaming to BigQuery (coming in future release)

## Quick Start

### Build from Source

```bash
# Clone the repository
cd slatr

# Build the project
sbt compile

# Create executable JAR
sbt cli/assembly

# The JAR will be at modules/cli/target/scala-2.13/slatr.jar
```

### Basic Usage

```bash
# Convert XML to JSON
java -jar modules/cli/target/scala-2.13/slatr.jar convert input.xml -f json -o output.json

# Convert to JSON Lines
java -jar modules/cli/target/scala-2.13/slatr.jar convert input.xml -f jsonl -o output.jsonl

# Preview inferred schema
java -jar modules/cli/target/scala-2.13/slatr.jar infer-schema input.xml

# Show XSD information
java -jar modules/cli/target/scala-2.13/slatr.jar xsd-info input.xml
```

## Commands

### convert

Convert XML file to modern formats.

```bash
slatr convert <input> [options]

Options:
  -o, --output <file>    Output file path (required)
  -f, --format <format>  Output format: json, jsonl (default: json)
  -c, --config <file>    Configuration file path
  --pretty               Pretty-print JSON output
  --validate             Validate XML against XSD schema
  --dry-run              Infer schema only, don't convert
```

**Examples:**

```bash
# Basic conversion
slatr convert books.xml -f json -o books.json

# With XSD validation
slatr convert data.xml -f jsonl -o data.jsonl --validate

# Using config file
slatr convert input.xml -c slatr.yaml

# Dry run to preview schema
slatr convert input.xml -f json --dry-run
```

### infer-schema

Infer and display schema from XML file.

```bash
slatr infer-schema <input>
```

**Example:**

```bash
slatr infer-schema examples/simple.xml

# Output:
# Root Element: catalog
# Fields: 5
#
#   title: StringType[]
#   author: StringType[]
#   year: IntType[]
#   price: DoubleType[]
#   available: BooleanType[]
```

### xsd-info

Display XSD information from XML file header.

```bash
slatr xsd-info <input>
```

**Example:**

```bash
slatr xsd-info examples/with-xsd.xml

# Output:
# XSD URL Found: http://example.com/schema/v1.xsd
# Attempting to download and parse XSD...
# ✓ Successfully downloaded and parsed XSD
#   Target Namespace: http://example.com/ns
#   Elements Defined: 15
```

## Configuration File

Create a `slatr.yaml` file to configure conversion behavior:

```yaml
input:
  path: "input.xml"
  encoding: "UTF-8"

schema:
  mode: "hybrid"  # auto, xsd, manual, hybrid
  
  xsd:
    enabled: true
    timeout: 30
    validate: false
    followImports: false
  
  sampling:
    size: 1000
  
  overrides:
    forceArrays:
      - "/root/items/item"
      - "/root/users/user"
    typeHints:
      "/root/timestamp": "timestamp"
      "/root/amount": "decimal"

chunking:
  enabled: false
  chunkSize: "128MB"
  preferBoundaries: true

output:
  format: "json"
  path: "output.json"
  pretty: true

logging:
  level: "info"
```

### Schema Modes

- **auto** - Auto-infer from XML sampling only
- **xsd** - Use XSD schema only (fails if XSD not available)
- **manual** - Use manual overrides only
- **hybrid** - Combine XSD + sampling + manual (recommended)

## XSD Auto-Download & Caching

slatr automatically detects and downloads XSD schemas referenced in XML headers:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<root xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
      xsi:schemaLocation="http://example.com/ns http://example.com/schema/v1.xsd">
  ...
</root>
```

**Features:**

- In-memory caching (per application run)
- HTTP download with configurable timeout
- Type inference from XSD types
- Array detection from `maxOccurs` attribute
- Optional XML validation against XSD

## Array Handling

slatr ensures consistent array handling, solving the common problem where single-item arrays are interpreted as strings:

**Input XML:**

```xml
<data>
  <record>
    <tags><tag>one</tag></tags>
  </record>
  <record>
    <tags>
      <tag>two</tag>
      <tag>three</tag>
    </tags>
  </record>
</data>
```

**Output JSON (always arrays):**

```json
[
  {"tags": [{"tag": "one"}]},
  {"tags": [{"tag": "two"}, {"tag": "three"}]}
]
```

Use `forceArrays` in config to explicitly mark paths as arrays.

## Architecture

```
slatr/
├── modules/
│   ├── core/              # Core library
│   │   ├── model/         # Data models (Schema, Field, DataType)
│   │   ├── parser/        # XML streaming parser
│   │   ├── schema/        # XSD resolver, schema inference
│   │   └── converter/     # Format converters (JSON, JSONL)
│   │
│   └── cli/               # CLI application
│       ├── commands/      # CLI commands
│       └── config/        # Config loader
│
└── examples/              # Example XML files and configs
```

## Development

### Prerequisites

- Java 11+
- Scala 2.13.12
- sbt 1.9.8

### Building

```bash
# Compile
sbt compile

# Run tests
sbt test

# Create fat JAR
sbt cli/assembly

# Format code
sbt scalafmt
```

### Running Examples

```bash
# Convert simple example
java -jar modules/cli/target/scala-2.13/slatr.jar convert examples/simple.xml -f json -o output.json --pretty

# Convert nested example
java -jar modules/cli/target/scala-2.13/slatr.jar convert examples/nested.xml -f jsonl -o output.jsonl

# Test single-item list handling
java -jar modules/cli/target/scala-2.13/slatr.jar convert examples/single-item-list.xml -f json -o output.json
```

## Roadmap

### MVP (Current)
- ✅ XML streaming parser
- ✅ XSD resolver with in-memory cache
- ✅ Schema inference (hybrid mode)
- ✅ JSON and JSONL converters
- ✅ CLI tool

### Phase 2 (Future)
- ⬜ Avro converter
- ⬜ Parquet converter
- ⬜ Unit and integration tests
- ⬜ Performance benchmarks

### Phase 3 (Future)
- ⬜ Apache Spark integration
- ⬜ Distributed chunking
- ⬜ Chunk-to-partition mapping

### Phase 4 (Future)
- ⬜ BigQuery streaming writer
- ⬜ PostgreSQL batch inserter
- ⬜ Docker container
- ⬜ CI/CD pipeline

## License

Apache License 2.0

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.

## Support

For issues, questions, or feature requests, please open an issue on GitHub.
