# XML to JSON Conversion CLI

## Overview

The XML to JSON conversion feature provides an interactive command-line interface for converting XML files to various output formats, including JSON, JSON Lines, and Parquet.

## Features

- Interactive file selection
- Multiple output format options
- Configurable conversion settings
- Schema preview (dry run)
- XML validation support
- Pretty-printing option

## Usage

### Basic Conversion

```bash
slatr convert
```

### Interactive Prompts

1. **Input File**
   - Enter the path to your XML file
   - Validates file existence and XML extension

2. **Output File**
   - Suggests a default output filename
   - Allows custom output file name

3. **Format Selection**
   - Choose from:
     1. JSON (default)
     2. JSON Lines
     3. Parquet

4. **Conversion Options**
   - Pretty-print JSON
   - XML validation
   - Dry run (schema preview)

## Examples

### Standard Conversion
```
$ slatr convert
🔍 XML to JSON Converter
-------------------------
Enter the path to your XML file: data.xml

Output file name (default: data.json): 

Select output format:
1. JSON (default)
2. JSON Lines
3. Parquet
Enter your choice (1-3): 

Additional Conversion Options:
Enable pretty-print JSON? (y/N): y
Validate XML against XSD? (y/N): n
Perform dry run (schema preview only)? (y/N): n

🔄 Converting data.xml to Json...
Inferring schema...
Converting...
✅ Successfully converted to data.json
```

### Dry Run (Schema Preview)
```
$ slatr convert
...
Perform dry run (schema preview only)? (y/N): y

📋 Dry run - schema preview:
  id: INTEGER
  name: STRING
  address: STRUCT[]
```

## Configuration

- Supports XSD validation
- Configurable output formats
- Flexible schema inference

## Troubleshooting

- Ensure input XML file exists
- Check file permissions
- Verify XML file structure