@echo off
echo 🚀 Complete nORM Performance Test Suite
echo =======================================

cd /d "%~dp0"

echo.
echo 🔧 Step 1: Building nORM with optimizations...
cd ..\src
dotnet build -c Release

if %ERRORLEVEL% neq 0 (
    echo ❌ nORM build failed
    pause
    exit /b 1
)

echo.
echo 🔧 Step 2: Building benchmark project...
cd ..\benchmarks
dotnet build -c Release

if %ERRORLEVEL% neq 0 (
    echo ❌ Benchmark build failed
    pause
    exit /b 1
)

echo.
echo ✅ Build completed successfully!
echo.
echo 🎯 What's been implemented:
echo.
echo ✅ EFFICIENT JOINS:
echo   • Full JOIN operation support
echo   • Optimized SQL generation
echo   • Support for complex projections
echo   • WHERE clause filtering on joins
echo.
echo ✅ OPTIMIZED BULK OPERATIONS:
echo   • SQLite-specific batching (900 param limit)
echo   • Transaction-based bulk processing
echo   • Prepared statement reuse
echo   • Smart batch size calculation
echo   • IN clause optimization for deletes
echo.
echo 📊 Expected Performance Gains:
echo   • Join operations: From 11ns placeholder to real competitive performance
echo   • Bulk operations: From 8.2M ns (16th place) to ~3-5M ns (top 5)
echo   • All other operations: Maintained at current levels
echo.

:menu
echo Choose test to run:
echo 1. Quick verification tests (includes JOIN tests)
echo 2. Fast nORM-only benchmarks  
echo 3. Full competitive benchmark suite
echo 4. Exit
echo.
set /p choice="Enter choice (1-4): "

if "%choice%"=="1" (
    echo.
    echo 🧪 Running quick verification tests...
    dotnet run -c Release -- --quick
    goto :menu
)

if "%choice%"=="2" (
    echo.
    echo ⚡ Running fast nORM-only benchmarks...
    dotnet run -c Release -- --fast
    goto :menu
)

if "%choice%"=="3" (
    echo.
    echo 🏁 Running full competitive benchmark...
    echo This will take 10-15 minutes and compare against EF Core, Dapper, and Raw ADO.NET
    echo.
    set /p confirm="Continue? (y/n): "
    if /i "%confirm%"=="y" (
        dotnet run -c Release
    )
    goto :menu
)

if "%choice%"=="4" (
    echo.
    echo 👋 Goodbye!
    goto :end
)

echo Invalid choice. Please try again.
goto :menu

:end
pause
