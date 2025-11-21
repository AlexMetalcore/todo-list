package main

import (
    "os"
	"fmt"
	"regexp"
	"strconv"
	"net/http"
	"database/sql"
	"github.com/gorilla/mux"
    "github.com/joho/godotenv"
    "github.com/thedevsaddam/renderer"
    _ "github.com/go-sql-driver/mysql"
    "github.com/AndyEverLie/go-pagination-bootstrap"
    "strings"
    "time"
    "context"
    "encoding/json"
    "github.com/segmentio/kafka-go"
)

var render *renderer.Render

var database *sql.DB
var dbName string
var dbHost string
var dbPort string
var errorEmail string

var kafkaWriter *kafka.Writer
var kafkaTopic string

type Post struct {
    Id  string
    Username  string
    Email string
    Content string
}

func init() {
	opts := renderer.Options {
		ParseGlobPattern: "./public/*.html",
	}
	render = renderer.New(opts)
}

func index(w http.ResponseWriter, r *http.Request) {
    var count int
    var limit int
    countPosts, err := database.Prepare("select count(*) as count from " + dbName + ".posts")

    if err != nil {
       fmt.Println(err)
    }

    err = countPosts.QueryRow().Scan(&count)

    if err != nil {
        fmt.Println(err)
    }

    rows, err := database.Query("select * from " + dbName + ".posts")

    if err != nil {
        fmt.Println(err)
    }

    defer rows.Close()
    postsData := []Post{}

    for rows.Next() {
        post := Post{}
        err := rows.Scan(&post.Id, &post.Username, &post.Email, &post.Content)
        if (err != nil) {
            fmt.Println(err)
            continue
        }
        postsData = append(postsData, post)
    }

    current, err := strconv.Atoi(r.FormValue("page"))
    pager := pagination.New(count, limit, current, "/")

    data := struct {
        Posts []Post
        Render *pagination.Pagination
        Email string
    } {Posts: postsData, Render: pager, Email: errorEmail}

	render.HTML(w, http.StatusOK, "home", data)
}

func add(w http.ResponseWriter, r *http.Request) {
	render.HTML(w, http.StatusOK, "add", nil)
}

func edit(w http.ResponseWriter, r *http.Request) {
    vars := mux.Vars(r)
    id := vars["id"]

    if id == "" {
       http.NotFound(w, r)
    }

    row := database.QueryRow("select * from " + dbName + ".posts where id = ?", id)
    post := Post{}
    err := row.Scan(&post.Id, &post.Username, &post.Email, &post.Content)

    data := struct {
        Post Post
    } {Post: post}

    if err != nil {
       fmt.Println(err)
       http.Error(w, http.StatusText(404), http.StatusNotFound)
    } else {
       render.HTML(w, http.StatusOK, "edit", data)
    }
}

func delete(w http.ResponseWriter, r *http.Request) {
    vars := mux.Vars(r)
    id := vars["id"]

    if id == "" {
        http.NotFound(w, r)
    }

    row := database.QueryRow("select * from " + dbName + ".posts where id = ?", id)
    post := Post{}
    err := row.Scan(&post.Id, &post.Username, &post.Email, &post.Content)

    if err != nil {
       fmt.Println(err)
       http.Error(w, http.StatusText(404), http.StatusNotFound)
    }

    if post.Id != "" {
        _, err := database.Exec("delete from " + dbName + ".posts where id = ?", id)
        if err != nil {
           http.Error(w, http.StatusText(404), http.StatusNotFound)
        }
        http.Redirect(w, r, "/", 301)
    }
}


func userData(w http.ResponseWriter, r *http.Request) {
    username := r.PostFormValue("username")
    email := r.PostFormValue("email")
    content := r.PostFormValue("content")

    if (username == "" || email == "" || content == "") {
        http.Redirect(w, r, "/add", 301)
    } else {
        if r.PostFormValue("id") != "" {
            id := r.PostFormValue("id")
            row := database.QueryRow("select * from " + dbName + ".posts where id = ?", id)
            post := Post{}
            err := row.Scan(&post.Id, &post.Username, &post.Email, &post.Content)

            if err != nil {
               fmt.Println(err)
               http.Error(w, http.StatusText(404), http.StatusNotFound)
            }

            if post.Id != "" {
                if m, _ := regexp.MatchString(`^([\w\.\_]{2,10})@(\w{1,}).([a-z]{2,4})$`, email); !m {
                     errorEmail = "Не верный формат e-mail " + email
                } else {
                    errorEmail = ""
                    res, err := database.Exec(
                        "update " + dbName + ".posts set username=?, email=?, content = ? where id = ?", username, email, content, post.Id,
                    )

                    if err != nil {
                       fmt.Println(err)
                    }

                    updateID, _ := res.LastInsertId()
                    publishPostCreated(updateID, username, email, content)
                }
            } else {
                if m, _ := regexp.MatchString(`^([\w\.\_]{2,10})@(\w{1,}).([a-z]{2,4})$`, email); !m {
                   errorEmail = "Не верный формат e-mail " + email
                } else {
                    errorEmail = ""
                    res, err := database.Exec(
                        "insert into " + dbName + ".posts (username, email, content) values (?, ?, ?)", username, email, content,
                    )

                    if err != nil {
                        fmt.Println(err)
                    }

                    newID, _ := res.LastInsertId()
                    publishPostCreated(newID, username, email, content)
                }
            }
        } else {
            if m, _ := regexp.MatchString(`^([\w\.\_]{2,10})@(\w{1,}).([a-z]{2,4})$`, email); !m {
                errorEmail = "Не верный формат e-mail " + email
            } else {
                res, err := database.Exec("insert into " + dbName + ".posts (username, email, content) values (?, ?, ?)",
                username, email, content)
                errorEmail = ""

                if err != nil {
                    fmt.Println(err)
                }

                newID, _ := res.LastInsertId()
                publishPostCreated(newID, username, email, content)
            }
        }

        http.Redirect(w, r, "/", 301)
    }
}

func publishPostCreated(id int64, username, email, content string) {
    if kafkaWriter == nil {
        fmt.Println("Kafka does not configure")
    }

    evt := map[string]interface{}{
        "event":       "post.created",
        "version":     1,
        "occurred_at": time.Now().UTC().Format(time.RFC3339Nano),
        "id":          id,
        "payload": map[string]string{
            "username": username,
            "email":    email,
            "content":  content,
        },
    }

    body, _ := json.Marshal(evt)

    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    if err := kafkaWriter.WriteMessages(ctx, kafka.Message{
        Key:   []byte(strconv.FormatInt(id, 10)),
        Value: body,
    }); err != nil {
        fmt.Println("kafka write error:", err)
    }
}

func createKafkaWriter() *kafka.Writer {
    brokers := os.Getenv("KAFKA_BROKERS")
    kafkaTopic = os.Getenv("KAFKA_TOPIC")

    if kafkaTopic == "" {
        kafkaTopic = "posts.created"
    }

    if strings.TrimSpace(brokers) == "" {
        return nil
    }

    return &kafka.Writer{
        Addr:         kafka.TCP(strings.Split(brokers, ",")...),
        Topic:        kafkaTopic,
        Balancer:     &kafka.Hash{},
        RequiredAcks: kafka.RequireAll,
    }
}

func main() {
    e := godotenv.Load()

	if e != nil {
		fmt.Print(e)
	}

	username := os.Getenv("DB_USER")
	password := os.Getenv("DB_PASS")
	dbName = os.Getenv("DB_NAME")
	dbHost = os.Getenv("DB_HOST")
	dbPort = os.Getenv("DB_PORT")

    db, err := sql.Open("mysql", "" + username + ":" + password + "@tcp(" + dbHost + ":" + dbPort + ")/" + dbName + "")

    if err != nil {
        fmt.Println(err)
    }

    database = db
    defer db.Close()
    
    kafkaWriter = createKafkaWriter()

    if kafkaWriter != nil {
       defer kafkaWriter.Close()
    }

	router := mux.NewRouter()
    router.PathPrefix("/assets/").Handler(http.StripPrefix("/assets/", http.FileServer(http.Dir("assets/"))))
    router.HandleFunc("/", index)
    router.HandleFunc("/add", add)
    router.HandleFunc("/edit/{id:[0-9]+}", edit)
    router.HandleFunc("/delete/{id:[0-9]+}", delete)
    router.HandleFunc("/userData", userData)
    port := ":8089"
    fmt.Println("Listening on port ", port)
    http.ListenAndServe(port, router)
}
