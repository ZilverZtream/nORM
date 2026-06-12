using System;
using System.Collections.Generic;
using System.Linq;
using System.Xml;
using System.Xml.Linq;

namespace nORM.Cli;

public static partial class ProviderMobilitySourceScanner
{
    private static bool TryScanProjectFile(string root, string file, List<ProviderMobilityFinding> findings)
    {
        try
        {
            var document = XDocument.Load(file, LoadOptions.SetLineInfo);
            foreach (var package in document.Descendants().Where(static element =>
                         element.Name.LocalName is "PackageReference" or "PackageVersion"))
            {
                var packageId = (string?)package.Attribute("Include") ?? (string?)package.Attribute("Update");
                if (string.IsNullOrWhiteSpace(packageId))
                    continue;

                foreach (var rule in ProjectRules)
                {
                    if (!packageId.Equals(rule.Pattern, StringComparison.OrdinalIgnoreCase))
                        continue;

                    AddFinding(
                        root,
                        file,
                        package is IXmlLineInfo lineInfo && lineInfo.HasLineInfo() ? lineInfo.LineNumber : 0,
                        rule,
                        findings);
                }
            }

            return true;
        }
        catch (XmlException)
        {
            return false;
        }
    }
}
