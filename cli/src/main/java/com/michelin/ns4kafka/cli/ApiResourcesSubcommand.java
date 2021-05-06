package com.michelin.ns4kafka.cli;

import com.michelin.ns4kafka.cli.services.ApiResourcesService;
import picocli.CommandLine;

import javax.inject.Inject;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "api-resources", description = "Print the supported API resources on the server")
public class ApiResourcesSubcommand implements Callable<Integer> {
    @Inject
    ApiResourcesService apiResourcesService;

    @Override
    public Integer call() {
        CommandLine.Help.TextTable tt = CommandLine.Help.TextTable.forColumns(
                CommandLine.Help.defaultColorScheme(CommandLine.Help.Ansi.AUTO),
                new CommandLine.Help.Column[]
                        {
                                new CommandLine.Help.Column(30, 2, CommandLine.Help.Column.Overflow.SPAN),
                                new CommandLine.Help.Column(30, 2, CommandLine.Help.Column.Overflow.SPAN),
                                new CommandLine.Help.Column(30, 2, CommandLine.Help.Column.Overflow.SPAN)
                        });
        tt.addRowValues("KIND", "NAMES", "NAMESPACED");
        apiResourcesService.getListResourceDefinition().forEach(rd ->
                tt.addRowValues(rd.getKind(), String.join(",", rd.getNames()), String.valueOf(rd.isNamespaced()))
        );
        System.out.println(tt);
        return 0;
    }
}
