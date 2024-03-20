package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"
)

func main() {
	// readInsertStatements()
	// ddlStatments()
	tablesWithFields()
}

type Table struct {
	Name   string
	Fields map[string][]string
}

func tablesWithFields() {
	// Open the SQL file
	file, err := os.Open("/home/sujit/Downloads/cleardb-eamitest/cleardb-structure.sql")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	// Create a new Scanner to read the file
	scanner := bufio.NewScanner(file)

	buf := make([]byte, 0, 64*1024*1024)
	scanner.Buffer(buf, 1000*1024*1024)

	// Use a custom split function to split lines based on newlines
	scanner.Split(bufio.ScanLines)

	// Regular expression pattern to match CREATE TABLE statements
	createTablePattern := regexp.MustCompile(`CREATE\s+TABLE\s+` + "`([^`]+)`" + `\s*\(([^;]+)\);`)
	fieldPattern := regexp.MustCompile("`([^`]+)`[^,]*")

	// Map to store table names and their field names with data types
	tableFields := make(map[string][]string)

	// Variable to hold CREATE TABLE statement
	var currentTableStmt string

	// Loop through each line of the SQL file
	for scanner.Scan() {
		line := scanner.Text()
		// Trim leading and trailing whitespaces
		line = strings.TrimSpace(line)

		// Concatenate lines until the semicolon is found
		currentTableStmt += line

		// Check if we've reached the end of the CREATE TABLE statement
		if strings.HasSuffix(line, ";") {
			// Check if it's a CREATE TABLE statement
			if matches := createTablePattern.FindStringSubmatch(currentTableStmt); len(matches) > 0 {
				tableName := matches[1]
				fields := matches[2]
				fmt.Println(tableName)
				// Extract field names
				fieldMatches := fieldPattern.FindAllStringSubmatch(fields, -1)
				var tableFieldList []string
				for _, match := range fieldMatches {
					fieldName := match[1]
					tableFieldList = append(tableFieldList, fieldName)
				}
				// Store table name and its fields
				tableFields[tableName] = tableFieldList
			}
			// Reset currentTableStmt
			currentTableStmt = ""
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	// Print table names along with their fields
	fmt.Println("Tables and their fields:")
	for tableName, fields := range tableFields {
		fmt.Printf("%s:\n", tableName)
		for _, field := range fields {
			fmt.Printf("\t%s\n", field)
		}
	}
}

func ddlStatments() {
	// Open the SQL file
	file, err := os.Open("/home/sujit/Downloads/cleardb-eamitest/cleardb-structure.sql")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	// Regular expression to match DDL statements (CREATE, ALTER, DROP, etc.)
	ddlRegex := regexp.MustCompile(`(?i)^\s*(CREATE|ALTER|DROP|TRUNCATE|RENAME|COMMENT|LOCK|UNLOCK|SET|GRANT|REVOKE|ANALYZE|INDEX|VACUUM|REFRESH|MATERIALIZED\s+VIEW)\s+(\S+)\s+.*?;`)

	// Initialize a map to store DDL statements by object name
	ddlStatements := make(map[string][]string)

	// Read the file line by line
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()

		// Match DDL statements
		if ddlRegex.MatchString(line) {
			matches := ddlRegex.FindStringSubmatch(line)
			ddlType := strings.ToUpper(matches[1])
			objectName := matches[2]

			// Append the DDL statement to the slice associated with the object name
			ddlStatements[objectName] = append(ddlStatements[objectName], line)

			// Optionally, you can print or process the matched DDL statement here
			fmt.Printf("Found %s statement for object %s: %s\n", ddlType, objectName, line)
		}
	}

	// Check for errors during scanning
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	// Print the DDL statements by object name
	for objectName, statements := range ddlStatements {
		fmt.Printf("DDL statements for object %s:\n", objectName)
		for _, statement := range statements {
			fmt.Println(statement)
		}
	}
}
func readInsertStatements() {
	// Open the SQL file
	file, err := os.Open("/home/sujit/Downloads/cleardb-eamitest/cleardb-data.sql")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	// Regular expression to match INSERT statements
	insertRegex := regexp.MustCompile(`(?i)INSERT\s+INTO\s+(\S+)\s+VALUES\s+\((.*?)\);`)
	insertRegex = regexp.MustCompile(`(?i)INSERT\s+INTO\s+(\S+)\s*(?:\(([^)]+)\))?\s*VALUES\s*\((.*?)\);`)
	// insertRegex = regexp.MustCompile(`(?i)INSERT\s+INTO\s+(\S+)\s+\((.*?)\)\s+VALUES\s+\((.*?)\);`)
	// Initialize a slice to store data
	insertData := make([]map[string]interface{}, 0)
	// Read the file line by line
	scanner := bufio.NewScanner(file)
	buf := make([]byte, 0, 64*1024*1024)
	scanner.Buffer(buf, 1000*1024*1024)

	// Use a custom split function to split lines based on newlines
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
		line := scanner.Text()
		// Match INSERT statement
		/*if strings.Contains(line, "INSERT INTO") {
			fmt.Println(line)
			panic(1)
		}*/
		if insertRegex.MatchString(line) {
			matches := insertRegex.FindStringSubmatch(line)
			tableName := matches[1]
			fieldNames := strings.Split(matches[2], ",")
			values := strings.Split(matches[3], "),(")
			fmt.Println(tableName, fieldNames, values)
			panic(1)
			// Process each set of values
			for _, v := range values {
				// Trim leading and trailing parentheses and spaces
				v = strings.TrimSpace(strings.Trim(v, "()"))

				// Split values by comma
				valueItems := strings.Split(v, ",")

				// Create a map to store data for this set of values
				data := make(map[string]interface{})
				for i, value := range valueItems {
					data[fmt.Sprintf("column_%d", i+1)] = strings.TrimSpace(value)
				}
				fmt.Println(len(data), data)
				// Append data to the slice
				insertData = append(insertData, data)
			}
			fmt.Println(tableName)
		}
	}
	fmt.Println(len(insertData))
	// Check for errors during scanning
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}

func readCreateStatements() {
	// Open the SQL file
	file, err := os.Open("/home/sujit/Downloads/cleardb-eamitest/cleardb-structure.sql")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	// Initialize a flag to track if we are inside a CREATE statement
	insideCreate := false

	// Initialize an empty string to store the current CREATE statement
	currentCreateStmt := ""

	// Read the file line by line
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()

		// Check if the line starts with "CREATE"
		if strings.HasPrefix(strings.TrimSpace(line), "CREATE") {
			insideCreate = true
			currentCreateStmt = line
		}

		// If inside a CREATE statement, continue adding lines until we encounter a semicolon
		if insideCreate {
			currentCreateStmt += line

			// If the line ends with a semicolon, the CREATE statement is complete
			if strings.HasSuffix(strings.TrimSpace(line), ";") {
				fmt.Println(currentCreateStmt)
				insideCreate = false
				currentCreateStmt = ""
			}
		}
	}

	// Check for errors during scanning
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}
