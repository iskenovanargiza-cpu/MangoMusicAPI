package com.mangomusic.dao;

import com.mangomusic.model.Album;
import com.mangomusic.model.Artist;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Repository;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Repository
public class AlbumDao {

    private final DataSource dataSource;

    public AlbumDao(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public List<Album> getRecentAlbums(int limit){
        List<Album> recentAlbums = new ArrayList<>();
        String query =  "SELECT a.* ,ar.name as artist_name " +
                "FROM albums a " +
                "JOIN artists ar ON a.artist_id = ar.artist_id " +
                "GROUP BY a.album_id " +
                "ORDER BY release_year DESC " +
                "LIMIT ?";

        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(query)) {

            statement.setInt(1, limit);

            try (ResultSet results = statement.executeQuery()) {
                while (results.next()) {
                    Album album = mapRowToAlbum(results);
                    recentAlbums.add(album);
                }
            }

        } catch (SQLException e) {
            throw new RuntimeException("Error getting albums by artist", e);
        }

        return recentAlbums;
    }




    public List<Album> getAllAlbums() {
        List<Album> albums = new ArrayList<>();
        String query = "SELECT al.album_id, al.artist_id, al.title, al.release_year, ar.name as artist_name " +
                "FROM albums al " +
                "JOIN artists ar ON al.artist_id = ar.artist_id " +
                "ORDER BY al.title";

        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(query);
             ResultSet results = statement.executeQuery()) {

            while (results.next()) {
                Album album = mapRowToAlbum(results);
                albums.add(album);
            }

        } catch (SQLException e) {
            throw new RuntimeException("Error getting all albums", e);
        }

        return albums;
    }

    public Album getPlayCount(int albumId){
        String query = "Select COUNT(ap.play_id), al.album_id, al.artist_id, al.title, al.release_year, " +
                "a.name as artist_name FROM albums al " +
                "JOIN artists a ON al.artist_id = a.artist_id " +
        "JOIN album_plays ap ON al.album_id = ap.album_id " +
        "WHERE al.album_id = 20";

        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(query)) {

            statement.setInt(1, albumId);

            try (ResultSet results = statement.executeQuery()) {
                if (results.next()) {
                    return mapRowToAlbum(results);
                }
            }

        } catch (SQLException e) {
            throw new RuntimeException("Error getting album by ID", e);
        }

        return null;
    }

    public Album getAlbumById(int albumId) {
        String query = "SELECT al.album_id, al.artist_id, al.title, al.release_year, ar.name as artist_name " +
                "FROM albums al " +
                "JOIN artists ar ON al.artist_id = ar.artist_id " +
                "WHERE al.album_id = ?";

        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(query)) {

            statement.setInt(1, albumId);

            try (ResultSet results = statement.executeQuery()) {
                if (results.next()) {
                    return mapRowToAlbum(results);
                }
            }

        } catch (SQLException e) {
            throw new RuntimeException("Error getting album by ID", e);
        }

        return null;
    }

    public List<Album> getAlbumsByArtist(int artistId) {
        List<Album> albums = new ArrayList<>();
        String query = "SELECT al.album_id, al.artist_id, al.title, al.release_year, ar.name as artist_name " +
                "FROM albums al " +
                "JOIN artists ar ON al.artist_id = ar.artist_id " +
                "WHERE al.artist_id = ? " +
                "ORDER BY al.release_year DESC";

        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(query)) {

            statement.setInt(1, artistId);

            try (ResultSet results = statement.executeQuery()) {
                while (results.next()) {
                    Album album = mapRowToAlbum(results);
                    albums.add(album);
                }
            }

        } catch (SQLException e) {
            throw new RuntimeException("Error getting albums by artist", e);
        }

        return albums;
    }

    public List<Album> getAlbumsByGenre(String genre) {
        List<Album> albums = new ArrayList<>();
        String query = "SELECT al.album_id, al.artist_id, al.title, al.release_year, ar.name as artist_name " +
                "FROM albums al " +
                "JOIN artists ar ON al.artist_id = ar.artist_id " +
                "WHERE ar.primary_genre = ? " +
                "ORDER BY al.title";

        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(query)) {

            statement.setString(1, genre);

            try (ResultSet results = statement.executeQuery()) {
                while (results.next()) {
                    Album album = mapRowToAlbum(results);
                    albums.add(album);
                }
            }

        } catch (SQLException e) {
            throw new RuntimeException("Error getting albums by genre", e);
        }

        return albums;
    }

    public List<Album> searchAlbums(String searchTerm) {
        List<Album> albums = new ArrayList<>();
        String query = "SELECT al.album_id, al.artist_id, al.title, al.release_year, ar.name as artist_name " +
                "FROM albums al " +
                "JOIN artists ar ON al.artist_id = ar.artist_id " +
                "WHERE al.title LIKE ? " +
                "ORDER BY al.title";

        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(query)) {

            statement.setString(1, "%" + searchTerm + "%");

            try (ResultSet results = statement.executeQuery()) {
                while (results.next()) {
                    Album album = mapRowToAlbum(results);
                    albums.add(album);
                }
            }

        } catch (SQLException e) {
            throw new RuntimeException("Error searching albums", e);
        }

        return albums;
    }

    public Album createAlbum(Album album) {
        String query = "INSERT INTO albums (artist_id, title, release_year) VALUES (?, ?, ?)";

        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS)) {

            statement.setInt(1, album.getArtistId());
            statement.setString(2, album.getTitle());
            statement.setObject(3, album.getReleaseYear());

            statement.executeUpdate();

            try (ResultSet keys = statement.getGeneratedKeys()) {
                if (keys.next()) {
                    int albumId = keys.getInt(1);
                    return getAlbumById(albumId);
                }
            }

        } catch (SQLException e) {
            throw new RuntimeException("Error creating album", e);
        }

        return null;
    }

    public Album updateAlbum(int albumId, Album album) {
        String query = "UPDATE albums SET artist_id = ?, title = ?, release_year = ? WHERE album_id = ?";

        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(query)) {

            statement.setInt(1, album.getArtistId());
            statement.setString(2, album.getTitle());
            statement.setObject(3, album.getReleaseYear());
            statement.setInt(4, albumId);

            int rowsAffected = statement.executeUpdate();
            if (rowsAffected == 0) {
                return null;
            }

            return getAlbumById(albumId);

        } catch (SQLException e) {
            throw new RuntimeException("Error updating album", e);
        }
    }

    public boolean deleteAlbum(int albumId) {
        String query = "DELETE FROM albums WHERE album_id = ?";

        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(query)) {

            statement.setInt(1, albumId);
            int rowsAffected = statement.executeUpdate();
            return rowsAffected > 0;

        } catch (SQLException e) {
            throw new RuntimeException("Error deleting album", e);
        }
    }

    private Album mapRowToAlbum(ResultSet results) throws SQLException {
        Album album = new Album();
        album.setAlbumId(results.getInt("album_id"));
        album.setArtistId(results.getInt("artist_id"));
        album.setTitle(results.getString("title"));
        album.setReleaseYear(results.getInt("release_year"));
        album.setArtistName(results.getString("artist_name"));
        return album;
    }
}