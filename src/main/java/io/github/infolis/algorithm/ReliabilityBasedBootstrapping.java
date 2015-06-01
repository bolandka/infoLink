package io.github.infolis.algorithm;

import io.github.infolis.infolink.luceneIndexing.PatternInducer;
import io.github.infolis.infolink.patternLearner.Reliability;
import io.github.infolis.model.Execution;
import io.github.infolis.model.ExecutionStatus;
import io.github.infolis.model.InfolisPattern;
import io.github.infolis.model.StudyContext;
import io.github.infolis.util.RegexUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.lucene.queryParser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author kata
 * @author domi
 */
public class ReliabilityBasedBootstrapping extends BaseAlgorithm {

    private static final Logger log = LoggerFactory.getLogger(ReliabilityBasedBootstrapping.class);

    private List<StudyContext> bootstrapReliabilityBased() throws IOException, ParseException {
        Set<String> reliableInstances = new HashSet<>();
        Set<StudyContext> extractedContexts = new HashSet<>();
        Set<String> processedPatterns = new HashSet<>();
        List<StudyContext> reliableContexts = new ArrayList<>();
        List<StudyContext> reliableContexts_iteration;
        int numIter = 1;
        Reliability r = new Reliability();
        Set<String> seeds = new HashSet<>();
        seeds.addAll(getExecution().getTerms());
        while (numIter < getExecution().getMaxIterations()) {
            log.debug("Bootstrapping... Iteration: " + numIter);
            // 0. filter seeds, select only reliable ones
            // alternatively: use all seeds extracted by reliable patterns
            reliableInstances.addAll(seeds);
            // 1. search for all seeds and save contexts
            for (String seed : seeds) {
                log.debug("Bootstrapping with seed " + seed);

                // 1. use lucene index to search for term in corpus
                Execution execution = new Execution();
                execution.setAlgorithm(SearchTermPosition.class);
                execution.setSearchTerm(seed);
                execution.setSearchQuery(RegexUtils.normalizeQuery(seed, true));
                execution.setInputFiles(getExecution().getInputFiles());
                execution.setThreshold(getExecution().getThreshold());

                execution.instantiateAlgorithm(getDataStoreClient(), getFileResolver()).run();

                List<StudyContext> detectedContexts = new ArrayList<>();
                for (String sC : execution.getStudyContexts()) {
                    StudyContext context = this.getDataStoreClient().get(StudyContext.class, sC);
                    //InfolisPattern pat = this.getDataStoreClient().get(InfolisPattern.class, context.getPattern());
                    detectedContexts.add(context);
                    // wrong
                    //processedPattern.add(pat.getPatternRegex());
                }
                //TODO: wrong: PatternInducer.saveReliablePatternData must be called for the context of each seed separately
                extractedContexts.addAll(detectedContexts);
            }
            // 2. get reliable patterns, save their data to this.reliablePatternsAndContexts and 
            // new seeds to this.foundSeeds_iteration
            //TODO: move inside of loop (see todo above)
            reliableContexts_iteration = PatternInducer.saveReliablePatternData(extractedContexts, getExecution().getThreshold(), processedPatterns, getExecution().getInputFiles().size(), reliableInstances, r);
            reliableContexts.addAll(reliableContexts_iteration);
            seeds = new HashSet<>();
            for (StudyContext sC : reliableContexts_iteration) seeds.add(sC.getTerm());
            numIter++;
        }
        return reliableContexts;
    }

    @Override
    public void execute() throws IOException {
        List<StudyContext> detectedContexts = new ArrayList<>();
        try {
            detectedContexts = bootstrapReliabilityBased();
        } catch (IOException | ParseException ex) {
            log.error("Could not apply reliability bootstrapping: " + ex);
            getExecution().setStatus(ExecutionStatus.FAILED);
        }

        for (StudyContext sC : detectedContexts) {
            getDataStoreClient().post(StudyContext.class, sC);
            this.getExecution().getStudyContexts().add(sC.getUri());
        }
        for (StudyContext sC : detectedContexts) {
            getDataStoreClient().post(StudyContext.class, sC);
            this.getExecution().getPattern().add(sC.getPattern());
        }
        getExecution().setStatus(ExecutionStatus.FINISHED);
    }

    @Override
    public void validate() {
        //TODO: what about the index path? need to be given!
        if (null == this.getExecution().getTerms()
                || this.getExecution().getTerms().isEmpty()) {
            throw new IllegalArgumentException("Must set at least one term as seed!");
        }
        if (null == this.getExecution().getInputFiles()
                || this.getExecution().getInputFiles().isEmpty()) {
            throw new IllegalArgumentException("Must set at least one input file!");
        }
    }

}