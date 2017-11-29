package netflix;

public class ViewMovie {
    String title;
    String description;
    int runtime;
    String releaseDate;
    float score;
    String cover;
    int id;
public  ViewMovie(String[] data){
    this.title = data[0];
    this.description = data[1];
    this.runtime = Integer.parseInt(data[2]);
    this.releaseDate = data[3];
    this.score = Float.parseFloat(data[4]);
    this.cover = data[5];
    this.id = Integer.parseInt(data[6]);
}
    public ViewMovie(String title, String description, int runtime, String releaseDate, float score, String filepath,String cover, int id) {
        this.title = title;
        this.description = description;
        this.runtime = runtime;
        this.releaseDate = releaseDate;
        this.score = score;
        this.cover = cover;
        this.id = id;
    }
    public int getId() {
        return id;
    }
    public String getTitle() {
        return title;
    }

    public String getDescription() {
        return description;
    }

    public int getRuntime() {
        return runtime;
    }

    public String getReleaseDate() {
        return releaseDate;
    }

    public float getScore() {
        return score;
    }

    public String getCover() {
        return cover;
    }
}
