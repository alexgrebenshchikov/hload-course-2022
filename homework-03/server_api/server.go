package server_api

import (
	"fmt"
	"net/http"

	_ "github.com/lib/pq"

	"github.com/gin-gonic/gin"

	"github.com/segmentio/kafka-go"

	dbi "main/db_interactor"

	gen "main/url_generator"

	kf "main/kafka"

	lr "main/local_redis"
)

type CreateRequest struct {
	Longurl string `json:"longurl"`
}

type CreateResponse struct {
	Longurl  string `json:"longurl"`
	Shorturl string `json:"shorturl"`
}

func putCreate(c *gin.Context, db *dbi.DbInteractor, urlWriter *kafka.Writer) {
	var request CreateRequest

	if err := c.BindJSON(&request); err != nil {
		return
	}

	url_id, short_url := gen.GenerateShortUrl(request.Longurl)
	for {
		var longurl, err = db.InsertURL(short_url, request.Longurl).Get()
		if err != nil {
			c.IndentedJSON(http.StatusConflict, gin.H{"message": "Short url generation failed"})
			break
		} else if request.Longurl != longurl {
			url_id = (url_id + 51) % gen.MAX_URL_NUMBER
			short_url = gen.IntToShortUrl(url_id)
		} else {
			go kf.UrlProduce(urlWriter, request.Longurl, short_url)
			var response = CreateResponse{request.Longurl, short_url}
			c.IndentedJSON(http.StatusCreated, response)
			break
		}
	}
}

func getLongURL(c *gin.Context) (string, int64) {
	shorturl := c.Param("shorturl")
	var longurl, clicks, err = lr.CheckTinyUrl(shorturl)
	fmt.Println(clicks)
	if err != nil {
		c.IndentedJSON(http.StatusNotFound, gin.H{"message": "url not found"})
	} else {
		c.Redirect(http.StatusFound, longurl)
	}
	return shorturl, clicks
}

func SetupMasterServerRouter(db *dbi.DbInteractor, urlWriter *kafka.Writer) *gin.Engine {
	router := gin.Default()
	router.PUT("/create", func(c *gin.Context) {
		putCreate(c, db, urlWriter)
	})
	return router
}

func SetupClickServerRouter(db *dbi.DbInteractor, urlReader *kafka.Reader, clickWriter *kafka.Writer) *gin.Engine {
	router := gin.Default()
	router.GET(":shorturl", func(c *gin.Context) {
		shorturl, clicks := getLongURL(c)
		if clicks%100 == 0 {
			kf.ClickProduce(clickWriter, shorturl, 100)
		}
	})
	return router
}
