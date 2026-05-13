import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import moa.classifiers.Classifier;
import moa.core.Example;
import moa.core.Utils;
import moa.options.ClassOption;
import moa.streams.ArffFileStream;

import com.yahoo.labs.samoa.instances.Instance;

/**
 * prequential vote logger for MOA.
 *
 * this runner preserves the test-then-train loop used by EvaluatePrequential,
 * but writes per-instance vote signals so downstream Python evaluators can
 * build threshold frontiers with continuous scores.
 */
public class MoaPrequentialVoteLogger {

    private static final int VOTE_COLUMNS = 8;

    private static final String USAGE =
        "Usage: MoaPrequentialVoteLogger "
        + "<input_arff> <learner_cli> <class_index> <instance_limit> <output_csv>";

    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            System.err.println(USAGE);
            System.exit(2);
        }

        final String inputArff = args[0];
        final String learnerCli = args[1];
        final int classIndex = Integer.parseInt(args[2]);
        final int instanceLimit = Integer.parseInt(args[3]);
        final String outputCsv = args[4];

        ArffFileStream stream = new ArffFileStream(inputArff, classIndex);
        stream.prepareForUse();

        Classifier learner = (Classifier) ClassOption.cliStringToObject(
            learnerCli,
            moa.classifiers.MultiClassClassifier.class,
            null
        );
        learner.setModelContext(stream.getHeader());
        learner.prepareForUse();

        File outputFile = new File(outputCsv);
        File parent = outputFile.getParentFile();
        if (parent != null) {
            parent.mkdirs();
        }

        long rowIndex = 0L;
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile, false))) {
            writer.write("row_index,true_label,predicted_label");
            for (int v = 0; v < VOTE_COLUMNS; v++) {
                writer.write(",vote_" + v);
            }
            writer.write(",positive_score\n");

            while (stream.hasMoreInstances() && (instanceLimit < 0 || rowIndex < instanceLimit)) {
                Example<Instance> example = stream.nextInstance();
                Instance instance = example.getData();

                int trueLabel = (int) instance.classValue();
                double[] votes = learner.getVotesForInstance(example);
                if (votes == null) {
                    votes = new double[0];
                }

                int predictedLabel = votes.length > 0 ? Utils.maxIndex(votes) : 0;
                double vote0 = votes.length > 0 ? votes[0] : 0.0;
                double vote1 = votes.length > 1 ? votes[1] : 0.0;
                double denom = vote0 + vote1;
                double positiveScore;
                if (Double.isFinite(denom) && denom > 0.0) {
                    positiveScore = vote1 / denom;
                } else {
                    positiveScore = predictedLabel == 1 ? 1.0 : 0.0;
                }

                writer.write(Long.toString(rowIndex));
                writer.write(",");
                writer.write(Integer.toString(trueLabel));
                writer.write(",");
                writer.write(Integer.toString(predictedLabel));
                for (int v = 0; v < VOTE_COLUMNS; v++) {
                    double vote = votes.length > v ? votes[v] : 0.0;
                    writer.write(",");
                    writer.write(Double.toString(vote));
                }
                writer.write(",");
                writer.write(Double.toString(positiveScore));
                writer.write("\n");

                learner.trainOnInstance(example);
                rowIndex += 1L;
            }
        } catch (IOException ioException) {
            System.err.println("Failed to write prediction votes: " + ioException.getMessage());
            throw ioException;
        }

        System.out.println("rows_written=" + rowIndex);
    }
}
