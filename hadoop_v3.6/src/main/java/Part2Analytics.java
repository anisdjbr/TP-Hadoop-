import java.io.IOException;
import java.time.Instant;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Part2Analytics {

    private static final Logger LOG = Logger.getLogger(Part2Analytics.class.getName());

    static {
        System.setProperty("java.util.logging.SimpleFormatter.format", "%5$s%n%6$s");
        try {
            FileHandler fh = new FileHandler("out.log");
            fh.setFormatter(new SimpleFormatter());
            LOG.addHandler(fh);
        } catch (Exception e) {
            System.exit(1);
        }
    }

    // =========================================================================
    // REQUÊTE 1 : FRÉQUENCE / PANIER / SATISFACTION PAR CLIENT
    //
    // Faits_Commandes.csv :
    // 0  ID_COMMANDE
    // 1  ID_CLIENT
    // 2  ID_RESTAURANT
    // 3  ID_LIVREUR
    // 4  ID_DATE
    // 5  ID_TEMPS
    // 6  ID_ZONE
    // 7  ID_PLAT
    // 8  PRIX_UNITAIRE
    // 9  FRAIS_LIVRAISON
    // 10 QUANTITÉ_PLAT
    // 11 DISTANCE_LIVRAISON
    // 12 DÉLAI_LIVRAISON
    // 13 NOTE_COMMANDE
    // 14 DATE_CRÉATION
    // =========================================================================
    public static class MapClientFrequency extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] fields = line.split(",");

            // détecter l'en-tête : "ID_COMMANDE",...
            String first = fields[0].replace("\"", "");
            if (first.equals("ID_COMMANDE")) return;

            if (fields.length <= 13) {
                LOG.warning("Ligne ignorée (trop peu de colonnes) : " + line);
                return;
            }

            String idClient      = fields[1].trim();
            String prixUnitaire  = fields[8].trim();
            String fraisLivraison= fields[9].trim();
            String quantite      = fields[10].trim();
            String noteCommande  = fields[13].trim();

            try {
                double prix  = Double.parseDouble(prixUnitaire);
                double frais = Double.parseDouble(fraisLivraison);
                double qty   = Double.parseDouble(quantite);
                double note  = Double.parseDouble(noteCommande);

                double montant = prix * qty + frais;

                // valeur : montant|note
                context.write(new Text(idClient),
                        new Text(montant + "|" + note));
            } catch (NumberFormatException e) {
                LOG.warning("Format error (Req1) : " + e.getMessage());
            }
        }
    }

    public static class ReduceClientFrequency extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            int nbCommandes = 0;
            double sommePanier = 0;
            double sommeSatisfaction = 0;

            for (Text val : values) {
                String[] parts = val.toString().split("\\|");
                if (parts.length == 2) {
                    double montant = Double.parseDouble(parts[0]);
                    double note    = Double.parseDouble(parts[1]);

                    sommePanier += montant;
                    sommeSatisfaction += note;
                    nbCommandes++;
                }
            }

            double panierMoyen = nbCommandes > 0 ? sommePanier / nbCommandes : 0;
            double satisfactionMoyenne = nbCommandes > 0 ? sommeSatisfaction / nbCommandes : 0;
            double valeurClientTotale = sommePanier;
            // approximation d'une fréquence annuelle (ici simple scaling)
            double frequenceAnnuelle = nbCommandes * 12.0;

            String result = String.format(
                    "Nb_Commandes=%d|Panier_Moyen=%.2f|Valeur_Totale=%.2f|Satisfaction=%.2f|Frequence_Annuelle=%.2f",
                    nbCommandes, panierMoyen, valeurClientTotale, satisfactionMoyenne, frequenceAnnuelle);

            context.write(new Text("Client_" + key.toString()), new Text(result));
        }
    }

    // =========================================================================
    // REQUÊTE 2 : PLATS LES PLUS COMMANDÉS
    //   (nb commandes, quantité totale, revenu, note moyenne, part % approximative)
    // Toujours Faits_Commandes.csv
    // =========================================================================
    public static class MapMostOrderedDishes extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] fields = line.split(",");
            String first = fields[0].replace("\"", "");
            if (first.equals("ID_COMMANDE")) return;

            if (fields.length <= 13) {
                LOG.warning("Ligne ignorée (trop peu de colonnes) : " + line);
                return;
            }

            String idPlat        = fields[7].trim();
            String prixUnitaire  = fields[8].trim();
            String quantite      = fields[10].trim();
            String noteCommande  = fields[13].trim();

            try {
                double prix = Double.parseDouble(prixUnitaire);
                double qty  = Double.parseDouble(quantite);
                double note = Double.parseDouble(noteCommande);

                // valeur : prix|quantité|note
                context.write(new Text(idPlat),
                        new Text(prix + "|" + qty + "|" + note));
            } catch (NumberFormatException e) {
                LOG.warning("Format error (Req2) : " + e.getMessage());
            }
        }
    }

    public static class ReduceMostOrderedDishes extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            int nbCommandes = 0;
            double quantiteTotale = 0;
            double revenuGenere = 0;
            double sommeNotes = 0;

            for (Text val : values) {
                String[] parts = val.toString().split("\\|");
                if (parts.length == 3) {
                    double prix = Double.parseDouble(parts[0]);
                    double qty  = Double.parseDouble(parts[1]);
                    double note = Double.parseDouble(parts[2]);

                    quantiteTotale += qty;
                    revenuGenere   += prix * qty;
                    sommeNotes     += note;
                    nbCommandes++;
                }
            }

            double noteMoyenne = nbCommandes > 0 ? sommeNotes / nbCommandes : 0;

            // Pourcentage approximatif (ici dataset d'exemple avec 5 commandes)
            double partCommandes = (nbCommandes * 100.0) / 5.0;

            String result = String.format(
                    "Nb_Commandes=%d|Quantite_Totale=%.0f|Revenu=%.2f|Note_Moyenne=%.2f|Part_Pct=%.2f",
                    nbCommandes, quantiteTotale, revenuGenere, noteMoyenne, partCommandes);

            context.write(new Text("Plat_" + key.toString()), new Text(result));
        }
    }

    // =========================================================================
    // REQUÊTE 3 : DÉLAI MOYEN DE LIVRAISON PAR RESTAURANT (vue globale)
    // Toujours Faits_Commandes.csv
    // =========================================================================
    public static class MapRestaurantDelay extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] fields = line.split(",");
            String first = fields[0].replace("\"", "");
            if (first.equals("ID_COMMANDE")) return;

            if (fields.length <= 13) {
                LOG.warning("Ligne ignorée (trop peu de colonnes) : " + line);
                return;
            }

            String idRestaurant  = fields[2].trim();
            String delai         = fields[12].trim();
            String noteCommande  = fields[13].trim();

            try {
                double delaiLivraison = Double.parseDouble(delai);
                double note           = Double.parseDouble(noteCommande);

                // valeur : delai|note
                context.write(new Text(idRestaurant),
                        new Text(delaiLivraison + "|" + note));
            } catch (NumberFormatException e) {
                LOG.warning("Format error (Req3) : " + e.getMessage());
            }
        }
    }

    public static class ReduceRestaurantDelay extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            int nbCommandes = 0;
            double sommDelai = 0;
            double sommSatisfaction = 0;
            int nbExcellentService = 0;

            for (Text val : values) {
                String[] parts = val.toString().split("\\|");
                if (parts.length == 2) {
                    double delai = Double.parseDouble(parts[0]);
                    double note  = Double.parseDouble(parts[1]);

                    sommDelai += delai;
                    sommSatisfaction += note;
                    if (note >= 4.5) nbExcellentService++;
                    nbCommandes++;
                }
            }

            double delaiMoyen = nbCommandes > 0 ? sommDelai / nbCommandes : 0;
            double satisfactionMoyenne = nbCommandes > 0 ? sommSatisfaction / nbCommandes : 0;
            double tauxSatisfactionElevee =
                    nbCommandes > 0 ? (nbExcellentService * 100.0) / nbCommandes : 0;

            String respectDelai = "Conforme";
            if (delaiMoyen > 25) respectDelai = "Legerement degrade";
            if (delaiMoyen > 30) respectDelai = "Non conforme";

            String result = String.format(
                    "Nb_Commandes=%d|Delai_Moyen=%.1f|Satisfaction_Reelle=%.2f|Respect_Delai=%s|Taux_Satisfaction_Elevee=%.1f",
                    nbCommandes, delaiMoyen, satisfactionMoyenne, respectDelai, tauxSatisfactionElevee);

            context.write(new Text("Restaurant_" + key.toString()), new Text(result));
        }
    }

    // =========================================================================
    // REQUÊTE 4 : RESTAURANTS SOUVENT EN RETARD
    //   -> même fichier Faits_Commandes, mais focus sur le taux de retard
    // =========================================================================
    public static class MapRestaurantLate extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] fields = line.split(",");
            String first = fields[0].replace("\"", "");
            if (first.equals("ID_COMMANDE")) return;

            if (fields.length <= 13) {
                LOG.warning("Ligne ignorée (trop peu de colonnes) : " + line);
                return;
            }

            String idRestaurant = fields[2].trim();
            String delai        = fields[12].trim();
            String noteCommande = fields[13].trim();

            try {
                double delaiLivraison = Double.parseDouble(delai);
                double note           = Double.parseDouble(noteCommande);

                // Ici on encode: delai|note
                context.write(new Text(idRestaurant),
                        new Text(delaiLivraison + "|" + note));
            } catch (NumberFormatException e) {
                LOG.warning("Format error (Req4) : " + e.getMessage());
            }
        }
    }

    public static class ReduceRestaurantLate extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            int nbCommandes = 0;
            int nbRetards = 0;
            double sommeDelai = 0;
            double sommeNotes = 0;

            for (Text val : values) {
                String[] parts = val.toString().split("\\|");
                if (parts.length == 2) {
                    double delai = Double.parseDouble(parts[0]);
                    double note  = Double.parseDouble(parts[1]);

                    sommeDelai += delai;
                    sommeNotes += note;
                    if (delai > 35.0) {
                        nbRetards++;
                    }
                    nbCommandes++;
                }
            }

            if (nbCommandes == 0) return;

            double delaiMoyen = sommeDelai / nbCommandes;
            double tauxRetardPct = (nbRetards * 100.0) / nbCommandes;
            double satisfactionClients = sommeNotes / nbCommandes;

            String niveauAlerte;
            if (nbRetards == 0) {
                niveauAlerte = "Pas de retard";
            } else if (tauxRetardPct < 10.0) {
                niveauAlerte = "Retards mineurs";
            } else if (tauxRetardPct < 25.0) {
                niveauAlerte = "Retards frequents";
            } else {
                niveauAlerte = "Probleme serieux";
            }

            String result = String.format(
                    "Nb_Commandes=%d|Delai_Moyen=%.1f|Nb_Retards=%d|Taux_Retard_Pct=%.1f|Satisfaction_Clients=%.2f|Niveau_Alerte=%s",
                    nbCommandes, delaiMoyen, nbRetards, tauxRetardPct, satisfactionClients, niveauAlerte);

            context.write(new Text("Restaurant_" + key.toString()), new Text(result));
        }
    }

    // =========================================================================
    // REQUÊTE 5 : PONCTUALITE LIVREUR
    //   -> Calcul sur Faits_Commandes (partie "réelle" de la requête SQL)
    // =========================================================================
    public static class MapLivreurPerformance extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] fields = line.split(",");
            String first = fields[0].replace("\"", "");
            if (first.equals("ID_COMMANDE")) return;

            if (fields.length <= 13) {
                LOG.warning("Ligne ignorée (trop peu de colonnes) : " + line);
                return;
            }

            String idLivreur       = fields[3].trim();
            String delai           = fields[12].trim();
            String distance        = fields[11].trim();
            String noteCommande    = fields[13].trim();

            try {
                double delaiLivraison = Double.parseDouble(delai);
                double dist           = Double.parseDouble(distance);
                double note           = Double.parseDouble(noteCommande);

                // Encode : delai|distance|note
                context.write(new Text(idLivreur),
                        new Text(delaiLivraison + "|" + dist + "|" + note));
            } catch (NumberFormatException e) {
                LOG.warning("Format error (Req5) : " + e.getMessage());
            }
        }
    }

    public static class ReduceLivreurPerformance extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            int nbLivraisons = 0;
            double sommeDelai = 0;
            double sommeDistance = 0;
            double sommeNotes = 0;
            int nbPonctuel = 0;         // delai <= 25
            int nbRetardCritique = 0;   // delai > 35

            for (Text val : values) {
                String[] parts = val.toString().split("\\|");
                if (parts.length == 3) {
                    double delai    = Double.parseDouble(parts[0]);
                    double distance = Double.parseDouble(parts[1]);
                    double note     = Double.parseDouble(parts[2]);

                    sommeDelai += delai;
                    sommeDistance += distance;
                    sommeNotes += note;

                    if (delai <= 25.0) nbPonctuel++;
                    if (delai > 35.0) nbRetardCritique++;
                    nbLivraisons++;
                }
            }

            if (nbLivraisons == 0) return;

            double delaiMoyen = sommeDelai / nbLivraisons;
            double distanceMoyenne = sommeDistance / nbLivraisons;
            double satisfactionClients = sommeNotes / nbLivraisons;

            double tauxPonctualitePct =
                    (nbPonctuel * 100.0) / nbLivraisons;
            double tauxRetardCritiquePct =
                    (nbRetardCritique * 100.0) / nbLivraisons;

            String evaluation;
            if (tauxPonctualitePct >= 80.0) {
                evaluation = "Excellent";
            } else if (tauxPonctualitePct >= 60.0) {
                evaluation = "Bon";
            } else {
                evaluation = "A ameliorer";
            }

            String result = String.format(
                    "Nb_Livraisons_Reelles=%d|Delai_Moyen=%.1f|Taux_Ponctualite_Pct=%.1f|Taux_Retard_Critique_Pct=%.1f|Distance_Moyenne=%.2f|Satisfaction_Clients=%.2f|Evaluation_Ponctualite=%s",
                    nbLivraisons, delaiMoyen, tauxPonctualitePct, tauxRetardCritiquePct,
                    distanceMoyenne, satisfactionClients, evaluation);

            context.write(new Text("Livreur_" + key.toString()), new Text(result));
        }
    }

    // =========================================================================
    // FACTORISATION : création d'un job générique
    // =========================================================================
    private static Job createJob(Configuration conf, String jobName, String inputPath,
                                 String outputPath, Class<? extends Mapper> mapperClass,
                                 Class<? extends Reducer> reducerClass)
            throws IOException {

        Job job = Job.getInstance(conf, jobName);
        job.setJarByClass(Part2Analytics.class);

        job.setMapperClass(mapperClass);
        job.setReducerClass(reducerClass);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(
                job, new Path(outputPath + Instant.now().getEpochSecond())
        );

        return job;
    }

    // =========================================================================
    // MAIN : enchaînement des 5 jobs analytiques
    // =========================================================================
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // Requête 1 : Fréquence / panier / satisfaction par client
        Job job1 = createJob(conf, "ClientFrequency",
                "input-partie2/Faits_Commandes.csv",
                "output/analytics-ClientFrequency-",
                MapClientFrequency.class, ReduceClientFrequency.class);
        job1.waitForCompletion(true);

        // Requête 2 : Plats les plus commandés
        Job job2 = createJob(conf, "MostOrderedDishes",
                "input-partie2/Faits_Commandes.csv",
                "output/analytics-Dishes-",
                MapMostOrderedDishes.class, ReduceMostOrderedDishes.class);
        job2.waitForCompletion(true);

        // Requête 3 : Délai moyen global par restaurant
        Job job3 = createJob(conf, "RestaurantDelay",
                "input-partie2/Faits_Commandes.csv",
                "output/analytics-RestaurantDelay-",
                MapRestaurantDelay.class, ReduceRestaurantDelay.class);
        job3.waitForCompletion(true);

        // Requête 4 : Restaurants souvent en retard
        Job job4 = createJob(conf, "RestaurantLate",
                "input-partie2/Faits_Commandes.csv",
                "output/analytics-RestaurantLate-",
                MapRestaurantLate.class, ReduceRestaurantLate.class);
        job4.waitForCompletion(true);

        // Requête 5 : Ponctualité livreurs
        Job job5 = createJob(conf, "LivreurPerformance",
                "input-partie2/Faits_Commandes.csv",
                "output/analytics-LivreurPerformance-",
                MapLivreurPerformance.class, ReduceLivreurPerformance.class);
        job5.waitForCompletion(true);

        System.out.println("✓ Les 5 jobs analytiques sont terminés !");
    }
}

