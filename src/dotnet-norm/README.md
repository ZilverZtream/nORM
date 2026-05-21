# dotnet-norm CLI

`dotnet-norm` provides command-line tooling for the nORM ORM framework. Use it to scaffold, validate, and manage nORM projects from the terminal.

## Features
- Validate connection strings before applying migrations.
- Generate boilerplate configuration for nORM projects.
- Offer helpful diagnostics to keep projects aligned with nORM best practices.

## Getting Started
Install the tool locally using the .NET CLI:

```bash
dotnet tool install --global dotnet-norm
```

Then run the tool from your project directory:

```bash
norm --help
```

The tool package follows the same versioning as the nORM repository release it ships with.
