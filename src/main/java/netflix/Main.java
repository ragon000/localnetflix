package netflix;


import com.mysql.jdbc.log.Slf4JLogger;
import info.movito.themoviedbapi.TmdbApi;
import info.movito.themoviedbapi.TmdbMovies;
import info.movito.themoviedbapi.TmdbPeople;
import info.movito.themoviedbapi.TmdbSearch;
import info.movito.themoviedbapi.model.Artwork;
import info.movito.themoviedbapi.model.Genre;
import info.movito.themoviedbapi.model.MovieDb;
import info.movito.themoviedbapi.model.ProductionCompany;
import info.movito.themoviedbapi.model.core.MovieKeywords;
import info.movito.themoviedbapi.model.core.MovieResultsPage;
import info.movito.themoviedbapi.model.keywords.Keyword;
import info.movito.themoviedbapi.model.people.Person;
import info.movito.themoviedbapi.model.people.PersonCast;
import info.movito.themoviedbapi.model.people.PersonCrew;
import info.movito.themoviedbapi.model.people.PersonPeople;
import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.sqlite.core.DB;
import spark.ModelAndView;
import spark.template.jade.JadeTemplateEngine;

import javax.swing.*;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;
import java.util.function.BiConsumer;

import static org.apache.commons.lang.StringEscapeUtils.escapeSql;
import static spark.Spark.*;

public class Main {
    DatabaseConnector dbconnector;
    TmdbApi movieapi;

    public Main(String dbfile, String path){

            if(!Files.exists(new File(dbfile).toPath()))
            createNewDatabase(new File(dbfile).getAbsolutePath().replace("\\","/"));

System.out.println(new File(dbfile).getAbsolutePath()+" --->   "+new File(dbfile).getAbsolutePath().replace("\\","/"));

            dbconnector = new DatabaseConnector("12",12,new File(dbfile).getAbsolutePath().replace("\\","/"),"12","12");

        dbconnector.executeStatement("CREATE TABLE IF NOT EXISTS `tags` (" +
                "        `tag`   TEXT" +
                ");");
        dbconnector.executeStatement("CREATE TABLE IF NOT EXISTS `studios` (" +
                "        `name`  TEXT" +
                ");");
        dbconnector.executeStatement("CREATE TABLE IF NOT EXISTS `persons` (" +
                "        `name`  TEXT," +
                "        `gender`        TEXT" +
                ");");
        dbconnector.executeStatement("CREATE TABLE IF NOT EXISTS `person_roles` (" +
                "        `title` INTEGER" +
                ");");
        dbconnector.executeStatement("INSERT INTO `person_roles` VALUES ('Actor');");
        dbconnector.executeStatement("CREATE TABLE IF NOT EXISTS `movies` (" +
                "        `title` TEXT," +
                "        `description`   TEXT," +
                "        `runtime`       INTEGER," +
                "        `release_date`  NUMERIC," +
                "        `score` INTEGER," +
                "        `filepath` TEXT," +
                "        `cover` TEXT" +
                ");");
        dbconnector.executeStatement("CREATE TABLE IF NOT EXISTS `movie_tags` (" +
                "        `movie_id`      INTEGER," +
                "        `tag_id`        INTEGER," +
                "        PRIMARY KEY(`movie_id`,`tag_id`)" +
                ") WITHOUT ROWID;");
        dbconnector.executeStatement("CREATE TABLE IF NOT EXISTS `movie_studios` (" +
                "        `movie_id`      INTEGER," +
                "        `studio_id`     INTEGER," +
                "        PRIMARY KEY(`movie_id`,`studio_id`)" +
                ") WITHOUT ROWID;");
        dbconnector.executeStatement("CREATE TABLE IF NOT EXISTS `movie_persons` (" +
                "        `movie_id`      INTEGER," +
                "        `person_id`     INTEGER," +
                "        `role_id`       INTEGER," +
                "        PRIMARY KEY(`movie_id`,`person_id`,`role_id`)" +
                ") WITHOUT ROWID;");
        dbconnector.executeStatement("CREATE TABLE IF NOT EXISTS `movie_genres` (" +
                "        `movie_id`      INTEGER," +
                "        `genre_id`      INTEGER," +
                "        PRIMARY KEY(`movie_id`,`genre_id`)" +
                ") WITHOUT ROWID;");
        dbconnector.executeStatement("CREATE TABLE IF NOT EXISTS `genres` (" +
                "        `genre` TEXT" +
                ");");

        port(8087);
        staticFiles.location("/public"); // Static files
        staticFiles.externalLocation(path);
        System.setProperty("file.seperator","/") ;
        get("/", (request, response) -> {
            dbconnector.executeStatement("Select *, rowid From movies ORDER BY title");
            Map<String,Object> map = new HashMap<>();
            List<ViewMovie> movies = new ArrayList<>();
            String[][] data = dbconnector.getCurrentQueryResult().getData();
            for(int i=0;i<dbconnector.getCurrentQueryResult().getRowCount();i++){
            System.out.println(Arrays.deepToString(dbconnector.getCurrentQueryResult().getColumnNames()));
                    movies.add(new ViewMovie(data[i][0],data[i][1],Integer.parseInt(data[i][2]),data[i][3],Float.parseFloat(data[i][4]),data[i][5],data[i][6],Integer.parseInt(data[i][7])));

                }
            map.put("movies", movies);

            return new ModelAndView(map, "index");

        },new JadeTemplateEngine());
        post("/", (request, response) -> {
            System.out.println(request.body().split("&")[0].split("=")[1]);
            dbconnector.executeStatement("Select *, rowid From movies WHERE title like '%"+request.body().split("&")[0].split("=")[1]+"%' ORDER BY title");
            Map<String,Object> map = new HashMap<>();
            List<ViewMovie> movies = new ArrayList<>();
            String[][] data = dbconnector.getCurrentQueryResult().getData();
            for(int i=0;i<dbconnector.getCurrentQueryResult().getRowCount();i++){
                System.out.println(Arrays.deepToString(dbconnector.getCurrentQueryResult().getColumnNames()));
                movies.add(new ViewMovie(data[i][0],data[i][1],Integer.parseInt(data[i][2]),data[i][3],Float.parseFloat(data[i][4]),data[i][5],data[i][6],Integer.parseInt(data[i][7])));

            }
            map.put("movies", movies);

            return new ModelAndView(map, "index");

        },new JadeTemplateEngine());
        get("/movie/:id", (request, response) -> {
            dbconnector.executeStatement("Select filepath from movies where rowid="+request.params("id"));
            String filepath = dbconnector.getCurrentQueryResult().getData()[0][0];
            Map<String,Object> map = new HashMap<>();
            map.put("movie",filepath);
            System.out.println("playing: "+filepath);
            return new ModelAndView(map, "play");
        },new JadeTemplateEngine());
        get("/serach", (request, response) -> {
            dbconnector.executeStatement("Select genre, rowid From genres Order BY genre");
            Map<String,Object> map = new HashMap<>();
            List<twoThings> genres = toList(dbconnector.getCurrentQueryResult().getData());
            map.put("genres", genres);
            dbconnector.executeStatement("Select tag, rowid From tags Order BY tag");
            List<twoThings> tags = toList(dbconnector.getCurrentQueryResult().getData());
            map.put("tags", tags);
            dbconnector.executeStatement("Select name, rowid From studios Order BY name");
            List<twoThings> studios = toList(dbconnector.getCurrentQueryResult().getData());
            map.put("studios", studios);
            dbconnector.executeStatement("Select name, rowid From persons Order BY name");
            List<twoThings> persons = toList(dbconnector.getCurrentQueryResult().getData());
            map.put("persons", persons);
            return new ModelAndView(map, "serach");

        },new JadeTemplateEngine());
        post("/serach", (request, response) -> {

            dbconnector.executeStatement("Select DISTINCT movies.*, movies.rowid From movies " +
                    "JOIN movie_genres ON movies.rowid = movie_genres.movie_id " +
                    "JOIN movie_studios ON movies.rowid = movie_studios.movie_id " +
                    "JOIN movie_tags ON movies.rowid = movie_tags.movie_id " +
                    generateWhereString(request.body()) +
                    "ORDER BY title");
            Map<String,Object> map = new HashMap<>();
            List<ViewMovie> movies = new ArrayList<>();
            String[][] data = dbconnector.getCurrentQueryResult().getData();
            for(int i=0;i<dbconnector.getCurrentQueryResult().getRowCount();i++){
                System.out.println(Arrays.deepToString(dbconnector.getCurrentQueryResult().getColumnNames()));
                movies.add(new ViewMovie(data[i][0],data[i][1],Integer.parseInt(data[i][2]),data[i][3],Float.parseFloat(data[i][4]),data[i][5],data[i][6],Integer.parseInt(data[i][7])));

            }
            map.put("movies", movies);

            return new ModelAndView(map, "index");

        },new JadeTemplateEngine());

        addData(path);
    }
    private String generateWhereString(String m){
        List<String> genres = new ArrayList<>();
        List<String> studios = new ArrayList<>();
        List<String> tags = new ArrayList<>();
        System.out.println(m.toString());
        String[] ms = m.split("&");
       for(String s : ms){
            String[] key = s.split("%3A");
            String[] keyinkey = key[1].split("=");
            if(keyinkey[1].equals("on")) {
                switch (key[0]) {
                    case "genre":
                        genres.add(keyinkey[0]);
                        break;
                    case "tag":
                        tags.add(keyinkey[0]);
                        break;
                    case "studio":
                        studios.add(keyinkey[0]);
                        break;
                    default:
                        break;
                }
            }
        }
        System.out.println(                "WHERE movie_genres.genre_id IN "+intlistToSqlString(genres)+
                " AND movie_tags.tag_id IN "+intlistToSqlString(tags)+
                " AND movie_studios.studio_id IN "+intlistToSqlString(studios)+" ");
        return
                "WHERE movie_genres.genre_id IN "+intlistToSqlString(genres)+
                        " AND movie_tags.tag_id IN "+intlistToSqlString(tags)+
                        " AND movie_studios.studio_id IN "+intlistToSqlString(studios)+" ";
    }
    private String intlistToSqlString(List<String> list){
        String toReturn = "( ";
        for(int i=0;i<list.size();i++){
            if(i!=list.size()-1)
                toReturn += ""+escapeSql(list.get(i))+", ";
            else
                toReturn += ""+escapeSql(list.get(i))+") ";
        }
        return toReturn;
    }
private List<twoThings> toList(String[][] s){
        List<twoThings> toReturn = new ArrayList<>();
        for(int i=0;i<s.length;i++){
            toReturn.add(new twoThings(s[i][0],s[i][1]));
        }
        return toReturn;
}
    private void addPerson(List<PersonCast> plist, int movieid){
        TmdbPeople t = movieapi.getPeople();
        System.out.println("Adding People from movie "+movieid);
        System.out.println(plist);
        plist.forEach(p -> {
        PersonPeople pp = t.getPersonInfo(p.getId());
            dbconnector.executeStatement("Select rowid from persons where name='"+escapeSql(pp.getName())+"' AND gender='"+escapeSql(pp.getHomepage())+"'");
            if(dbconnector.getCurrentQueryResult().getRowCount()==0){
                System.out.println("ist null"+escapeSql(pp.getHomepage()));
                dbconnector.executeStatement("Insert into persons Values ('"+escapeSql(pp.getName())+"', '"+escapeSql(pp.getHomepage())+"')");
                System.out.println("Insert into persons Values ('"+escapeSql(pp.getName())+"', "+escapeSql(pp.getHomepage())+"')");
                dbconnector.executeStatement("Select rowid from persons where name ='"+escapeSql(pp.getName())+"' AND gender='"+escapeSql(pp.getHomepage())+"'");
            }
            System.out.println(Arrays.deepToString(dbconnector.getCurrentQueryResult().getData()));
            int personid = Integer.parseInt(dbconnector.getCurrentQueryResult().getData()[0][0]);
        dbconnector.executeStatement("Select rowid from person_roles where title='Actor'");
        int roleid = Integer.parseInt(dbconnector.getCurrentQueryResult().getData()[0][0]);
        dbconnector.executeStatement("Insert into movie_persons Values ("+movieid+","+personid+","+roleid+")");
        });
    }
    private void addPersoncrew(List<PersonCrew> plist, int movieid){
        TmdbPeople t = movieapi.getPeople();
        System.out.println("Adding People from movie "+movieid);
        plist.forEach(p -> {

            PersonPeople pp = t.getPersonInfo(p.getId());
            dbconnector.executeStatement("Select rowid from persons where name ='"+escapeSql(pp.getName())+"' AND gender='"+escapeSql(pp.getHomepage())+"'");
            if(dbconnector.getCurrentQueryResult().getRowCount()==0){
            dbconnector.executeStatement("Insert into persons Values ('"+escapeSql(pp.getName())+"', '"+escapeSql(pp.getHomepage())+"')");
            System.out.println("Insert into persons Values ('"+escapeSql(pp.getName())+"', "+escapeSql(pp.getHomepage())+"')");
            dbconnector.executeStatement("Select rowid from persons where name ='"+escapeSql(pp.getName())+"' AND gender='"+escapeSql(pp.getHomepage())+"'");
            }
            int personid = Integer.parseInt(dbconnector.getCurrentQueryResult().getData()[0][0]);
            dbconnector.executeStatement("Select rowid from person_roles where title='"+p.getJob()+"'");
            if(dbconnector.getCurrentQueryResult().getRowCount()==0) {
                dbconnector.executeStatement("INSERT INTO person_roles VALUES ('"+p.getJob()+"')");
                dbconnector.executeStatement("Select rowid from person_roles where title='"+p.getJob()+"'");
            }
                int roleid = Integer.parseInt(dbconnector.getCurrentQueryResult().getData()[0][0]);
            dbconnector.executeStatement("Insert into movie_persons Values ("+movieid+","+personid+","+roleid+")");
        });
    }
    private void addStudio(List<ProductionCompany> plist, int movieid){
        System.out.println("Adding Companies from movie "+movieid);
        plist.forEach(com -> {
            dbconnector.executeStatement("Select rowid from studios where name='"+escapeSql(com.getName())+"'");
            if(dbconnector.getCurrentQueryResult().getRowCount()==0) {
                dbconnector.executeStatement("INSERT INTO studios VALUES ('"+escapeSql(com.getName())+"')");
                dbconnector.executeStatement("Select rowid from studios where name='"+escapeSql(com.getName())+"'");
            }
            int comid = Integer.parseInt(dbconnector.getCurrentQueryResult().getData()[0][0]);
            dbconnector.executeStatement("Insert into movie_studios Values ("+movieid+","+comid+")");
        });
    }
    private void addGenre(List<Genre> plist, int movieid){
        System.out.println("Adding Genres from movie "+movieid);
        plist.forEach(gen -> {
            dbconnector.executeStatement("Select rowid from genres where genre='"+escapeSql(gen.getName())+"'");
            if(dbconnector.getCurrentQueryResult().getRowCount()==0) {
                dbconnector.executeStatement("INSERT INTO genres VALUES ('"+escapeSql(gen.getName())+"')");
                dbconnector.executeStatement("Select rowid from genres where genre='"+escapeSql(gen.getName())+"'");
            }
            int genid = Integer.parseInt(dbconnector.getCurrentQueryResult().getData()[0][0]);
            dbconnector.executeStatement("Insert into movie_genres Values ("+movieid+","+genid+")");
        });
    }
    private void addTag(List<Keyword> plist, int movieid){
        System.out.println("Adding Genres from movie "+movieid);
        plist.forEach(gen -> {
            dbconnector.executeStatement("Select rowid from tags where tag='"+escapeSql(gen.getName())+"'");
            if(dbconnector.getCurrentQueryResult().getRowCount()==0) {
                dbconnector.executeStatement("INSERT INTO tags VALUES ('"+escapeSql(gen.getName())+"')");
                dbconnector.executeStatement("Select rowid from tags where tag='"+escapeSql(gen.getName())+"'");
            }
            int genid = Integer.parseInt(dbconnector.getCurrentQueryResult().getData()[0][0]);
            dbconnector.executeStatement("Insert into movie_tags Values ("+movieid+","+genid+")");
        });
    }
    private void addData(String path){
        movieapi = new TmdbApi(yourcodehere);

    TmdbSearch serach = movieapi.getSearch();
    TmdbMovies dbmovies = movieapi.getMovies();
    Collection<File> f;
    f = FileUtils.listFiles(new File(path), TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE);
    f.forEach(file -> {
        System.out.println("adding movie "+ file.getName().split("\\.")[0]);
        try {
            dbconnector.executeStatement("Select rowid from movies where filepath='" + file.getCanonicalPath().substring(path.length()) + "'");
        }catch (IOException e) {
            e.printStackTrace();
        }
        if(dbconnector.getCurrentQueryResult().getRowCount()==0) {
            List<MovieDb> dbl = serach.searchMovie(file.getName().split("\\.")[0], null, "de-DE", true, 0).getResults();

            MovieDb m1 = dbl.get(0);
            MovieDb m = dbmovies.getMovie(m1.getId(), "de-DE", TmdbMovies.MovieMethod.credits, TmdbMovies.MovieMethod.keywords);

                dbconnector.executeStatement("Insert INTO movies VALUES " + movieToString(m, file.getAbsolutePath().substring(new File(path).getAbsolutePath().length())));

            dbconnector.executeStatement("Select rowid From movies where cover='" + m.getPosterPath() + "'");
            int rowid = Integer.parseInt(dbconnector.getCurrentQueryResult().getData()[0][0]);
            addPerson(m.getCast(), rowid);
            addPersoncrew(m.getCrew(), rowid);
            addStudio(m.getProductionCompanies(), rowid);
            addGenre(m.getGenres(), rowid);
            addTag(m.getKeywords(), rowid);
        }
    } );



}
    private String movieToString(MovieDb m, String path){
        System.out.println("('"+m.getTitle()+"', '"+m.getOverview()+"', "+m.getRuntime()+", '"+m.getReleaseDate()+"', "+m.getVoteAverage()+"', '"+escapeSql(path)+"', '"+m.getPosterPath()+"')");
        return  "('"+escapeSql(m.getTitle())+"', '"+escapeSql(m.getOverview())+"', "+escapeSql(m.getRuntime()+"")+", '"+escapeSql(m.getReleaseDate())+"', "+escapeSql(m.getVoteAverage()+"")+", '"+escapeSql(path)+"', '"+escapeSql(m.getPosterPath())+"')";
    }

public static void main(String args[]) throws ClassNotFoundException {
        Class.forName("org.sqlite.JDBC");
    Options options = new Options();

    Option input = new Option("d", "database", true, "Database File Path");
    input.setRequired(true);
    options.addOption(input);

    Option output = new Option("p", "path", true, "Path with movies");
    output.setRequired(true);
    options.addOption(output);

    CommandLineParser parser = new DefaultParser();
    HelpFormatter formatter = new HelpFormatter();
    CommandLine cmd;

    try {
        cmd = parser.parse(options, args);
    } catch (ParseException e) {
        System.out.println(e.getMessage());
        formatter.printHelp("utility-name", options);

        System.exit(1);
        return;
    }

    String inputFilePath = cmd.getOptionValue("database");
    String outputFilePath = cmd.getOptionValue("path");

    System.out.println(inputFilePath);
    System.out.println(outputFilePath);

new Main(inputFilePath, outputFilePath);

}
private static void createNewDatabase(String fileName) {

        String url = "jdbc:sqlite:" + fileName;

        try (Connection conn = DriverManager.getConnection(url)) {
            if (conn != null) {
                DatabaseMetaData meta = conn.getMetaData();
                System.out.println("The driver name is " + meta.getDriverName());
                System.out.println("A new database has been created.");
            }

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

}
