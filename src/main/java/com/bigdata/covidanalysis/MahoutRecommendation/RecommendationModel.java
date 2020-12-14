/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bigdata.covidanalysis.MahoutRecommendation;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

/**
 *
 * @author ruchit
 */
public class RecommendationModel {
    
    public static void Recommend(String path) throws IOException, TasteException {

        try {
            File userPreferencesFile = new File(path);

            DataModel dataModel = new FileDataModel(userPreferencesFile);

            UserSimilarity userSimilarity = new PearsonCorrelationSimilarity(dataModel);

            UserNeighborhood userNeighborhood = new NearestNUserNeighborhood(5, userSimilarity, dataModel);
            // Create a generic user based recommender with the dataModel, the userNeighborhood and the userSimilarity
            Recommender genericRecommender = new GenericUserBasedRecommender(dataModel,
                    userNeighborhood, userSimilarity);

            for (LongPrimitiveIterator iterator = dataModel.getUserIDs(); iterator.hasNext();) {
                long userId = iterator.nextLong();

                // 3 recommendations for each user
                List<RecommendedItem> itemRecommendations = genericRecommender.recommend(userId, 3);

                if (!itemRecommendations.isEmpty()) {
                    System.out.format("Id: %d%n", userId);
                    for (RecommendedItem recommendedItem : itemRecommendations) {
                        System.out.format(" Recommendation: %d. Strength of preference: %f%n",
                                recommendedItem.getItemID(), recommendedItem.getValue());
                    }
                }
            }

        } catch (IOException ex) {

            ex.printStackTrace();

        } catch (TasteException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }
}
