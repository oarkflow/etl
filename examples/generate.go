package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	metadata "github.com/oarkflow/metadata/v2"
)

const tableName = "users"
const numRows = 10000000
const batchSize = 500

func main() {
	f := gofakeit.New(0)
	fieldNames := []string{
		"first_name",
		"last_name",
		"email",
		"username",
		"password", // Use secure hashing for real applications
		"address",
		"city",
		"state",
		"country",
		"postal_code",
		"phone_number",
		"website",
		"company",
		"job_title",
		"birthday",
		"bio",
		"interests",
		"profile_picture",
		"created_at",
		"updated_at",
	}
	fields := strings.Join(fieldNames, ", ")
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(fieldNames)), ",")
	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES(%s)", tableName, fields, placeholders)
	cfg1 := metadata.Config{
		Host:          "localhost",
		Port:          3306,
		Driver:        "mysql",
		Username:      "root",
		Password:      "T#sT1234",
		Database:      "tests",
		DisableLogger: true,
	}
	src := metadata.New(cfg1)
	sr, err := src.Connect()
	if err != nil {
		panic(err)
	}
	for i := 0; i < numRows; i++ {
		var values []any
		for _, field := range fieldNames {
			switch field {
			case "email":
				values = append(values, f.Email())
			case "username":
				values = append(values, f.Username())
			case "password":
				values = append(values, f.LoremIpsumWord()) // Replace with secure hashing
			case "address":
				values = append(values, f.Address().Street)
			case "city":
				values = append(values, f.Address().City)
			case "state":
				values = append(values, f.Address().State)
			case "country":
				values = append(values, f.Address().Country)
			case "postal_code":
				values = append(values, f.Address().Zip)
			case "phone_number":
				values = append(values, f.Phone())
			case "website":
				values = append(values, f.URL())
			case "company":
				values = append(values, f.Company())
			case "job_title":
				values = append(values, f.JobTitle())
			case "birthday":
				values = append(values, f.Date().Format("2006-01-02"))
			case "bio":
				values = append(values, f.LoremIpsumParagraph(2, 3, 10, "."))
			case "interests":
				values = append(values, fmt.Sprintf("%s, %s, %s", f.BuzzWord(), f.BuzzWord(), f.BuzzWord()))
			case "profile_picture":
				values = append(values, f.ImageURL(200, 200))
			case "created_at", "updated_at":
				values = append(values, time.Now().Format("2006-01-02 15:04:05"))
			default:
				values = append(values, f.LoremIpsumWord())
			}
		}
		err := sr.Exec(sql, values...)
		if err != nil {
			panic(err)
		}
	}
}
