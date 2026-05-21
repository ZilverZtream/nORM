@echo off
setlocal EnableExtensions

set "ROOT=%~dp0.."
pushd "%ROOT%" >nul

set "CONFIGURATION=%NORM_GATE_CONFIGURATION%"
if not defined CONFIGURATION set "CONFIGURATION=Release"

set "MODE=%~1"
if "%MODE%"=="" set "MODE=full"

set "LIVE_FILTER=FullyQualifiedName~LiveProvider|FullyQualifiedName~ProviderParity|FullyQualifiedName~ProviderBehaviorEquivalenceTests|FullyQualifiedName~ProviderBindingParityTests|FullyQualifiedName~BulkProviderParityTests|FullyQualifiedName~CrossProviderAdversarialTests|FullyQualifiedName~LiveCrossProviderTests|FullyQualifiedName~ProviderDmlMigrationParityTests|FullyQualifiedName~ProviderSwapSmokeTests|FullyQualifiedName~SqlServerMigrationRunnerTests|FullyQualifiedName~PostgresMigrationRunnerTests|FullyQualifiedName~BulkTempTableLeakTests|FullyQualifiedName~PostgresBulkDuplicateTests"

set /a LIVE_COUNT=0
if defined NORM_TEST_SQLSERVER set /a LIVE_COUNT+=1
if not defined NORM_TEST_SQLSERVER if defined NORM_TEST_SQLSERVER_CS set /a LIVE_COUNT+=1
if defined NORM_TEST_MYSQL set /a LIVE_COUNT+=1
if not defined NORM_TEST_MYSQL if defined NORM_TEST_MYSQL_CS set /a LIVE_COUNT+=1
if defined NORM_TEST_POSTGRES set /a LIVE_COUNT+=1
if not defined NORM_TEST_POSTGRES if defined NORM_TEST_POSTGRES_CS set /a LIVE_COUNT+=1

if not defined NORM_MIN_LIVE_PROVIDERS set "NORM_MIN_LIVE_PROVIDERS=%LIVE_COUNT%"
if not defined NORM_REQUIRE_LIVE_PARITY if %LIVE_COUNT% GTR 0 set "NORM_REQUIRE_LIVE_PARITY=any"

echo nORM live provider gate
echo   Mode:          %MODE%
echo   Configuration: %CONFIGURATION%
echo   Live providers configured: %LIVE_COUNT%
echo   NORM_MIN_LIVE_PROVIDERS: %NORM_MIN_LIVE_PROVIDERS%
if defined NORM_REQUIRE_LIVE_PARITY echo   NORM_REQUIRE_LIVE_PARITY: %NORM_REQUIRE_LIVE_PARITY%
echo.

if /i "%MODE%"=="build" goto build
if /i "%MODE%"=="quick" goto quick
if /i "%MODE%"=="live" goto live
if /i "%MODE%"=="full" goto full

echo Unknown mode "%MODE%".
echo Usage: eng\live-provider-gate.cmd [build^|quick^|live^|full]
popd >nul
exit /b 2

:build
call :run dotnet restore nORM.sln
if errorlevel 1 goto fail
call :run dotnet build nORM.sln --no-restore -c %CONFIGURATION% --nologo
if errorlevel 1 goto fail
goto done

:quick
call :run dotnet build nORM.sln -c %CONFIGURATION% --nologo
if errorlevel 1 goto fail
call :run dotnet test tests\nORM.Tests.csproj --no-build -c %CONFIGURATION% --filter "FullyQualifiedName~ProviderSwapSmokeTests" --logger "console;verbosity=normal"
if errorlevel 1 goto fail
goto done

:live
call :run dotnet build nORM.sln -c %CONFIGURATION% --nologo
if errorlevel 1 goto fail
call :run dotnet test tests\nORM.Tests.csproj --no-build -c %CONFIGURATION% --filter "%LIVE_FILTER%" --logger "console;verbosity=normal"
if errorlevel 1 goto fail
goto done

:full
call :run dotnet restore nORM.sln
if errorlevel 1 goto fail
call :run dotnet build nORM.sln --no-restore -c %CONFIGURATION% --nologo
if errorlevel 1 goto fail
call :run dotnet test tests\nORM.Tests.csproj --no-build -c %CONFIGURATION% --logger "console;verbosity=normal"
if errorlevel 1 goto fail
call :run dotnet test tests\nORM.Tests.csproj --no-build -c %CONFIGURATION% --filter "%LIVE_FILTER%" --logger "console;verbosity=normal"
if errorlevel 1 goto fail
goto done

:run
echo ^> %*
%*
exit /b %ERRORLEVEL%

:fail
set "EXITCODE=%ERRORLEVEL%"
echo.
echo nORM live provider gate failed with exit code %EXITCODE%.
popd >nul
exit /b %EXITCODE%

:done
echo.
echo nORM live provider gate passed.
popd >nul
exit /b 0
