package flowdistribute;

import java.util.List;

public interface ComputeDiffTimeOfSection {
    List<SectionOfTimeCollection> compute(LogitResultToSectionCompute logitResult);
}
