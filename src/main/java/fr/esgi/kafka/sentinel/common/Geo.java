package fr.esgi.kafka.sentinel.common;

/** Calculs geographiques. */
public final class Geo {

    private static final double EARTH_RADIUS_KM = 6371.0;

    private Geo() {
    }

    /**
     * Distance du grand cercle (Haversine) entre deux points, en km.
     * L'approximation plane est refusee par le sujet : 1 degre de longitude
     * ne couvre pas la meme distance a Paris qu'a l'equateur, seule la
     * formule spherique donne la vraie distance a vol d'oiseau.
     */
    public static double haversineKm(double lat1, double lon1, double lat2, double lon2) {
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(dLon / 2) * Math.sin(dLon / 2);
        double c = 2 * Math.asin(Math.min(1.0, Math.sqrt(a)));
        return EARTH_RADIUS_KM * c;
    }
}
