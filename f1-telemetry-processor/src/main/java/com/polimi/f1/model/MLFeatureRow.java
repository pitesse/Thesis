package com.polimi.f1.model;

// denormalized feature row for ml training: one row per driver per lap.
// combines lap timing/tire data with positional gap data computed by rival identification.
// exported to ml_features/ csv sink for offline model training.
//
// this flat representation gives the ml model the full state matrix of the race,
// allowing it to learn when conditions (tire age, gaps, weather) become optimal
// for a pit stop, rather than learning only from isolated alert events.
public class MLFeatureRow {

    public static final String CSV_HEADER
            = "race,driver,lapNumber,position,compound,tyreLife,trackTemp,airTemp,"
            + "humidity,rainfall,speedTrap,team,gapAhead,gapBehind,lapTime,pitLoss,trackStatus";

    private String race;
    private String driver;
    private int lapNumber;
    private int position;
    private String compound;
    private int tyreLife;
    private Double trackTemp;
    private Double airTemp;
    private Double humidity;
    private Boolean rainfall;
    private Double speedTrap;
    private String team;
    private Double gapAhead;
    private Double gapBehind;
    private Double lapTime;
    private Double pitLoss;
    private String trackStatus;

    public MLFeatureRow() {
    }

    public MLFeatureRow(String race, String driver, int lapNumber, int position,
            String compound, int tyreLife, Double trackTemp, Double airTemp,
            Double humidity, Boolean rainfall, Double speedTrap, String team,
            Double gapAhead, Double gapBehind, Double lapTime, Double pitLoss,
            String trackStatus) {
        this.race = race;
        this.driver = driver;
        this.lapNumber = lapNumber;
        this.position = position;
        this.compound = compound;
        this.tyreLife = tyreLife;
        this.trackTemp = trackTemp;
        this.airTemp = airTemp;
        this.humidity = humidity;
        this.rainfall = rainfall;
        this.speedTrap = speedTrap;
        this.team = team;
        this.gapAhead = gapAhead;
        this.gapBehind = gapBehind;
        this.lapTime = lapTime;
        this.pitLoss = pitLoss;
        this.trackStatus = trackStatus;
    }

    public String getRace() { return race; }
    public void setRace(String race) { this.race = race; }

    public String getDriver() { return driver; }
    public void setDriver(String driver) { this.driver = driver; }

    public int getLapNumber() { return lapNumber; }
    public void setLapNumber(int lapNumber) { this.lapNumber = lapNumber; }

    public int getPosition() { return position; }
    public void setPosition(int position) { this.position = position; }

    public String getCompound() { return compound; }
    public void setCompound(String compound) { this.compound = compound; }

    public int getTyreLife() { return tyreLife; }
    public void setTyreLife(int tyreLife) { this.tyreLife = tyreLife; }

    public Double getTrackTemp() { return trackTemp; }
    public void setTrackTemp(Double trackTemp) { this.trackTemp = trackTemp; }

    public Double getAirTemp() { return airTemp; }
    public void setAirTemp(Double airTemp) { this.airTemp = airTemp; }

    public Double getHumidity() { return humidity; }
    public void setHumidity(Double humidity) { this.humidity = humidity; }

    public Boolean getRainfall() { return rainfall; }
    public void setRainfall(Boolean rainfall) { this.rainfall = rainfall; }

    public Double getSpeedTrap() { return speedTrap; }
    public void setSpeedTrap(Double speedTrap) { this.speedTrap = speedTrap; }

    public String getTeam() { return team; }
    public void setTeam(String team) { this.team = team; }

    public Double getGapAhead() { return gapAhead; }
    public void setGapAhead(Double gapAhead) { this.gapAhead = gapAhead; }

    public Double getGapBehind() { return gapBehind; }
    public void setGapBehind(Double gapBehind) { this.gapBehind = gapBehind; }

    public Double getLapTime() { return lapTime; }
    public void setLapTime(Double lapTime) { this.lapTime = lapTime; }

    public Double getPitLoss() { return pitLoss; }
    public void setPitLoss(Double pitLoss) { this.pitLoss = pitLoss; }

    public String getTrackStatus() { return trackStatus; }
    public void setTrackStatus(String trackStatus) { this.trackStatus = trackStatus; }

    // ex: Italian Grand Prix,VER,25,1,MEDIUM,15,42.3,28.1,45.0,false,342.5,Red Bull Racing,3.2,1.5,81.234,25.0,1
    public String toCsvRow() {
        return String.join(",",
                race != null ? race : "",
                driver != null ? driver : "",
                String.valueOf(lapNumber),
                String.valueOf(position),
                compound != null ? compound : "",
                String.valueOf(tyreLife),
                trackTemp != null ? String.format("%.1f", trackTemp) : "",
                airTemp != null ? String.format("%.1f", airTemp) : "",
                humidity != null ? String.format("%.1f", humidity) : "",
                rainfall != null ? String.valueOf(rainfall) : "",
                speedTrap != null ? String.format("%.1f", speedTrap) : "",
                team != null ? team : "",
                gapAhead != null ? String.format("%.3f", gapAhead) : "",
                gapBehind != null ? String.format("%.3f", gapBehind) : "",
                lapTime != null ? String.format("%.3f", lapTime) : "",
                pitLoss != null ? String.format("%.1f", pitLoss) : "",
                trackStatus != null ? trackStatus : ""
        );
    }

    @Override
    public String toString() {
        return String.format("ML | %s | %s Lap %d P%d | %s L%d | Gap: +%.1fs -%.1fs | %.3fs",
                race != null ? race : "?",
                driver, lapNumber, position,
                compound, tyreLife,
                gapAhead != null ? gapAhead : 0.0,
                gapBehind != null ? gapBehind : 0.0,
                lapTime != null ? lapTime : 0.0);
    }
}
