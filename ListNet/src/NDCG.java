import scala.Serializable;

import java.util.Arrays;
import java.util.List;

public class NDCG  {

    /*
    public static void main(String[] args) {
        List<Integer> urls = Arrays.asList(new Integer[] { 2, 3, 1, 4, 5 });
        List<Integer> oracleUrls = Arrays.asList(new Integer[] { 1, 2, 3, 4, 5 });

        System.out.println(NDCG.getNDCG(urls, oracleUrls, 5));
    }
    */

    public static double getNDCG(List<Integer> urls, List<Integer> oracleUrls, int r) {
        // get DCG of urls
        double urlDCG = getDCG(urls, oracleUrls, r);

        // get DCG of perfect ranking
        double perfectDCG = getDCG(oracleUrls, oracleUrls, r);

        // normalize by dividing
        double normalized = urlDCG / perfectDCG;
        return normalized;
    }

    private static double getDCG(List<Integer> urls, List<Integer> oracleUrls, int p) {

        double score = 0;
        if(p > urls.size())
            p = urls.size();

        for (int i = 0; i < p; i++) {
            double relevance = getRelevance(urls.get(i), oracleUrls);
            int ranking = i + 1;

            if (ranking > 1) {
                // for all positions after the first one, reduce the "gain" as ranking increases
                relevance /= logBase2(ranking);
            }

            score += relevance;
        }

        return score;
    }

    private static double getRelevance(Integer url, List<Integer> oracleUrls) {
        // Use the position in the oracle ranking as the relevance
        return oracleUrls.size() - oracleUrls.indexOf(url);
    }

    private static double logBase2(double value) {
        return Math.log(value) / Math.log(2);
    }
}