# dm

dm is micro data mapper for Go. dm performs queries and maps results
to Plain Old Go Structs (POGS). dm was inspired by the excellent
[Dapper](https://github.com/DapperLib/Dapper) library for .NET.

## Features

- Use Go's standard [sql](https://pkg.go.dev/database/sql) package
- Works with POGS
- Supports joins and nested POGS
- Does not require special fields, names, embedded structs, or tags
- No special setup or configuration needed
