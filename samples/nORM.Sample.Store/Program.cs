using nORM.Sample.Store;

if (StoreSampleProgram.IsVerificationCommand(args))
{
    var exitCode = await StoreSampleProgram.RunAsync(args);
    return exitCode;
}

await StoreWebApp.RunAsync(args);
return 0;
