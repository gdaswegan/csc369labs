public class QueueNode implements Comparable<QueueNode> {
   private String word;
   private Integer weight;

   public QueueNode(String word, Integer weight){
      this.word = word;
      this.weight = weight;
   }

   @Override
   public int compareTo(QueueNode o) {
      return weight.compareTo(o.weight);
   }

   public int getWeight () { return weight; }
   public String getWord () { return word; }
}

