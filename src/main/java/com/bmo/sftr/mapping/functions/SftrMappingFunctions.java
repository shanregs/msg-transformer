package com.bmo.sftr.mapping.functions;

import static com.bmo.sftr.mapping.model.OutputColumns.CP_BRANCH_OF_REP_CP;
import static com.bmo.sftr.mapping.model.OutputColumns.CP_CP_SIDE;
import static com.bmo.sftr.mapping.model.OutputColumns.CP_NATURE_OF_REP_CP;
import static com.bmo.sftr.mapping.model.OutputColumns.CP_REP_CP;
import static com.bmo.sftr.mapping.model.OutputColumns.CP_SEC_OF_REP_CP;

import com.bmo.sftr.mapping.model.MappingFunction;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import org.springframework.stereotype.Component;

@Component
public class SftrMappingFunctions {

    private final Map<String, MappingFunction> byName = new LinkedHashMap<>();

    public SftrMappingFunctions() {

        // ===== Trade =====
        register(CP_CP_SIDE, (msg, ctx) ->
                ctx.constants().lookup("cpSide", MappingFunction.text(msg, "trade_type")));

        // ===== Reporting counterparty (reference-data enrichment) =====
        register(CP_REP_CP, (msg, ctx) ->
                RefDataExtractors.partyLei(ctx.refData(), ctx.constants()));

        register(CP_NATURE_OF_REP_CP, (msg, ctx) -> firstNonBlank(
                RefDataExtractors.partyNature(ctx.refData(), ctx.constants()),
                ctx.constants().value("natureOfRepCpDefault")));

        register(CP_SEC_OF_REP_CP, (msg, ctx) ->
                RefDataExtractors.partySector(ctx.refData(), ctx.constants()));

        // ===== Branch logic =====
        register(CP_BRANCH_OF_REP_CP, (msg, ctx) -> {
            String legalEntity = MappingFunction.text(msg, "bmoLegalEntity");
            return ctx.constants().startsWithAny("londonLegalEntityPrefixes", legalEntity)
                    ? ctx.constants().value("londonBranchCountry")
                    : null;
        });

        // add new lambdas here, grouped by business area, in spreadsheet order
    }

    public Optional<MappingFunction> find(String name) {
        return Optional.ofNullable(byName.get(name));
    }

    /** protected so tests can register extra lambdas in a subclass. */
    protected void register(String name, MappingFunction fn) {
        if (byName.putIfAbsent(name, fn) != null) {
            throw new IllegalStateException("Duplicate mapping function: " + name);
        }
    }

    private static String firstNonBlank(String... candidates) {
        for (String c : candidates) if (c != null && !c.isBlank()) return c;
        return null;
    }
}
