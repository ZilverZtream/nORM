@echo off
echo ========================================
echo        nORM Performance Benchmarks
echo ========================================
echo.

cd /d "%~dp0"

if "%1"=="--quick" (
    echo Running quick functionality tests...
    echo.
    dotnet run -c Release -- --quick
    goto :end
)

if "%1"=="--fast" (
    echo Running fast nORM-only benchmarks...
    echo.
    dotnet run -c Release -- --fast
    goto :end
)

if "%1"=="--help" (
    echo Usage:
    echo   run-benchmarks.bat          Run full benchmark suite
    echo   run-benchmarks.bat --quick  Run quick functionality test
    echo   run-benchmarks.bat --fast   Run fast nORM-only benchmarks
    echo   run-benchmarks.bat --help   Show this help
    echo.
    echo Full benchmarks take 10-15 minutes to complete.
    echo Quick tests take 30 seconds and verify functionality.
    echo Fast benchmarks take 2-3 minutes and test nORM only.
    goto :end
)

echo This will run comprehensive performance benchmarks comparing:
echo   - nORM
echo   - Entity Framework Core  
echo   - Dapper
echo   - Raw ADO.NET
echo.
echo The benchmark will take 10-15 minutes to complete.
echo Results will be saved in BenchmarkDotNet.Artifacts/results/
echo.
echo Press any key to continue, or Ctrl+C to cancel...
pause >nul

echo.
echo Starting benchmarks...
dotnet run -c Release

:end
echo.
echo Press any key to exit...
pause >nul
