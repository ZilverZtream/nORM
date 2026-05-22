# Security Policy

## Supported Versions

Until v1.0, security fixes are applied to `main` and the latest preview package.
After v1.0, the latest minor release line is supported for security fixes. Older
minor versions receive fixes only when explicitly announced in release notes.

## Reporting a Vulnerability

Do not open a public issue for a suspected vulnerability. Email the maintainer at
the security contact listed on the NuGet package or use GitHub private
vulnerability reporting when it is enabled for the repository.

Please include:

- affected nORM version and provider,
- a minimal reproduction,
- whether credentials, tenant isolation, raw SQL, migrations, or generated code
  are involved,
- expected impact and known workarounds.

The project will acknowledge valid reports, coordinate a fix, and publish a
release note that describes impact and upgrade guidance without exposing exploit
details before users can patch.
