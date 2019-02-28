import java.util.List;
import java.util.stream.Collectors;

public class ComputeDice {
   public static double compute (List<String> words1, List<String> words2){
      double n = words1.size();
      double m = words2.size();
      List<String> commonWords = words1.stream()
         .filter( w -> words1.contains(w) && words2.contains(w))
         .collect(Collectors.toList());

      double numCommon = commonWords.size();

      return 2 * numCommon /(n + m);
   }
}
