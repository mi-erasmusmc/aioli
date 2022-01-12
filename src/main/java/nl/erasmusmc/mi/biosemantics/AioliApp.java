package nl.erasmusmc.mi.biosemantics;


import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.stream.Collectors;

public class AioliApp {

    private static final String TARGET_VOCAB = "target-vocab";
    private static final Logger log = LogManager.getLogger(AioliApp.class.getName());
    public static final String RETAIN_MULTI_INGREDIENTS = "retain-multi-ingredients";
    public static final String SKIP_NORMALIZER = "skip-normalizer";


    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        log.info("Starting the Aioli");
        Arguments arguments = getArguments(args);
        Mapper mapper = new Mapper(arguments.getVocab(), arguments.getRetainMulti(), arguments.getSkipNormalizer());
        mapper.map();
        long minutes = (System.currentTimeMillis() - start) / 60000;
        log.info("Done! in {} minutes", minutes);
    }

    private static Arguments getArguments(String[] args) {

        Options options = new Options();
        Option tv = new Option("tv", TARGET_VOCAB, true, "target mapping vocabulary");
        tv.setRequired(true);
        options.addOption(tv);
        Option rmin = new Option("rmin", RETAIN_MULTI_INGREDIENTS, true, "retain multi ingredient codes");
        rmin.setRequired(true);
        options.addOption(rmin);
        Option sn = new Option("sn", SKIP_NORMALIZER, true, "skip the rxnav api");
        sn.setRequired(true);
        options.addOption(sn);
        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            log.error(e.getMessage());
            formatter.printHelp("utility-name", options);
            System.exit(1);
        }
        String tvInput = cmd.getOptionValue(TARGET_VOCAB);
        String rminInput = cmd.getOptionValue(RETAIN_MULTI_INGREDIENTS);
        String snInput = cmd.getOptionValue(SKIP_NORMALIZER);
        Vocab vocab = null;
        Boolean retainMulti = null;
        Boolean skipNormalizer = null;
        try {
            vocab = Vocab.valueOf(tvInput.toUpperCase());
            retainMulti = BooleanUtils.toBooleanObject(rminInput);
            skipNormalizer = BooleanUtils.toBooleanObject(snInput);
            log.info("Mapping to {}", vocab);
            if (retainMulti == null) {
                log.error("{} is not a valid boolean for retain multi", rminInput);
                System.exit(1);
            }
            if (skipNormalizer == null) {
                log.error("{} is not a valid boolean for skip normalizers", skipNormalizer);
                System.exit(1);
            }
        } catch (IllegalArgumentException e) {
            String validTargets = Arrays.stream(Vocab.values()).map(Enum::toString).collect(Collectors.joining(", "));
            log.error("{} is not a valid target, valid targets are {}", tvInput, validTargets);
            System.exit(1);
        }
        return new Arguments(vocab, retainMulti, skipNormalizer);
    }

    public enum Vocab {
        ATC,
        RXNORM,
        OMOP
    }
}

@Getter
@AllArgsConstructor
class Arguments {
    private AioliApp.Vocab vocab;
    private Boolean retainMulti;
    private Boolean skipNormalizer;
}
