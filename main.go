// main package implements an in-memory key-value stote database that can be interacted
// with using the Standard Output and Standard Input. The package implements the
// following methods: GET, SET, DELETE, BEGIN, ROLLBACK, COMMIT, END
package main

import (
	"bufio"
	"fmt"
	"github.com/teejays/clog"
	"os"
	"strings"
)

// STDIN_PROMPT is the prompt text that is used when interacting with the db
const STDIN_PROMPT string = ">> "

// LOG_LEVEL controls how much logging is printed to Std. out and/or Syslog. Higher the number,
// less is stuff logged. A default value of 5 means nothing will be probably logged in this application.
const LOG_LEVEL int = 5

// Error variables for various unexpected situations related to dealing with the Std. In interaction
var (
	ERR_INVALID_COMMAND_KEYWORD error = fmt.Errorf("INVALID COMMAND")
	ERR_INVALID_ARGS_NUM        error = fmt.Errorf("INVALID NUMBER OF ARGUMENTS PROVIDED")
	ERR_STMT_EMPTY              error = fmt.Errorf("EMPTY STATEMENT PROVIDED")
)

// EnableTestMode controls the behavior of the END command. If set to true, the END command
// doesn't call os.Exit() to kill the process. This is required since the testing process still
// has to continue after running END.
var EnableTestMode bool = false

// ActionHandler holds the neccesary information/logic that is required to handle different commands
// such as GET, SET etc. to the appropriate handler functions. It also includes field for NumArgs that
// helps validate whether the request for a given action is valid or not.
type ActionHandler struct {
	Fn      func(...string) (string, error) // Function that handles the logic for a particular command. It takes a variable number args (since this can vary per handler).
	NumArgs int                             // Number of arguments
}

// funcMap maps different commands such as GET, SET etc. to the apporpriate ActionHandler.
var funcMap map[string]ActionHandler = map[string]ActionHandler{
	"SET":      ActionHandler{handleSet, 2},
	"GET":      ActionHandler{handleGet, 1},
	"DELETE":   ActionHandler{handleDelete, 1},
	"COUNT":    ActionHandler{handleCount, 1},
	"BEGIN":    ActionHandler{handleBegin, 0},
	"ROLLBACK": ActionHandler{handleRollback, 0},
	"COMMIT":   ActionHandler{handleCommit, 0},
	"END":      ActionHandler{handleEnd, 0},
}

func main() {
	// Set the log level for the logging package, Clog, that we're using.
	// Clog is my own colored logging package for Go.
	clog.LogLevel = LOG_LEVEL

	// Initialize the Store
	InitStore()

	// Start the Std. In/Out interaction
	initializeStdInInterface()
}

// initializeStdInInterface starts the standard output/input interface to interact with the program.
func initializeStdInInterface() {
	// To work with the standard input, we'll use the Go's bufio package that provides
	// and Scanner interface for reading newline delimited text.
	scanner := bufio.NewScanner(os.Stdin)

	for {
		// Show the prompt each time we expect a text input
		fmt.Print(STDIN_PROMPT)

		// Scan for new text, this will hold until enter is pressed on the Std. In. or
		// if there is an error.
		scanner.Scan()

		// If there is an error while scanning, print it.
		if err := scanner.Err(); err != nil {
			fmt.Println(err)
			continue
		}

		// Get the entire statement from the Scanner as a string.
		stmt := scanner.Text()

		// Process the statement to interact with the data storage, and get the output to be printed.
		output, err := processStatement(stmt)
		if err == ERR_STMT_EMPTY {
			continue
		}
		if err != nil {
			fmt.Println(err)
		}
		// Print the output to the Std. out
		if output != "" {
			fmt.Println(output)
		}
	}
}

// processStatement takes the entire line string that has been sent through Std. In. and
// process it. It does that by first spliting the statement into the command (first word)
// and args (following words), and then calls the apporpriate handlers for that command.
// It returns the output to be printed, and returns an error if an error is encoutered.
func processStatement(stmt string) (string, error) {
	clog.Debugf("Processing Statement: %s", stmt)

	// Do some basic sanity checks to make sure the statement makes sense.
	err := validateStatement(stmt)
	if err != nil {
		return "", err
	}

	// Clean up the statement for extra whitespace of edges
	stmt = strings.TrimSpace(stmt)

	// Extract the main command and the args from the statement
	cmd, args := extractCommandArgs(stmt)

	// Get the action handler for the detected command. If no action handler is found,
	// return err.
	h, exists := funcMap[cmd]
	if !exists {
		return "", ERR_INVALID_COMMAND_KEYWORD
	}

	// Make sure that the number of args that we have received are same as what we expect for this command.
	if len(args) != h.NumArgs {
		return "", ERR_INVALID_ARGS_NUM
	}

	// Call the handler function for the command, with the args, to interact with the data store
	// and get the output to be printed.
	output, err := h.Fn(args...)
	if err != nil {
		return output, err
	}

	clog.Debugf("CurrentStore: %v", currentStore)
	return output, nil
}

/******************************************************************************
* H A N D L E R S
*******************************************************************************/
// These handler functions wrap around the struct methods defined for type Store.
// The mainly just extract the args and call Store methods with the args.

// handleSet handles SET commands from Std. Input.
func handleSet(args ...string) (string, error) {
	key := args[0]
	value := args[1]

	err := GetCurrentStore().Set(key, value)
	return "", err
}

// handleSet handles GET commands from Std. Input.
func handleGet(args ...string) (string, error) {
	key := args[0]

	val, err := GetCurrentStore().Get(key)
	if err == ERR_KEY_NOT_EXIST || val == "" {
		return "NULL", nil
	}
	return val, err
}

// handleSet handles DELETE commands from Std. Input.
func handleDelete(args ...string) (string, error) {
	key := args[0]

	err := GetCurrentStore().Delete(key)
	return "", err
}

// handleSet handles COUNT commands from Std. Input.
func handleCount(args ...string) (string, error) {
	val := args[0]

	cnt, err := GetCurrentStore().Count(val)
	return fmt.Sprintf("%d", cnt), err
}

// handleSet handles BEGIN commands from Std. Input.
func handleBegin(args ...string) (string, error) {
	err := GetCurrentStore().Begin()
	return "", err
}

// handleSet handles ROLLBACK commands from Std. Input.
func handleRollback(args ...string) (string, error) {
	err := GetCurrentStore().Rollback()
	return "", err
}

// handleSet handles COMMIT commands from Std. Input.
func handleCommit(args ...string) (string, error) {
	err := GetCurrentStore().Commit()
	return "", err
}

// handleSet handles END commands from Std. Input. Depending on the EnableTestMode flag,
// it either exits from the process or refreshes the store to a blank state.
func handleEnd(args ...string) (string, error) {
	// Refresh the store to a blank state
	InitStore()
	if !EnableTestMode {
		os.Exit(0)
	}
	return "", nil
}

// validateStatement runs sanity checks to make sure that the statement seems valid. Currently,
// the only check enabled is the one that checks for emptiness.
func validateStatement(q string) error {
	if strings.TrimSpace(q) == "" {
		return ERR_STMT_EMPTY
	}
	return nil
}

// extractCommandArgs takes a statement in the form of a string and returns the action command
// such as GET, SET, DELETE etc., and all the provided args in the statement.
func extractCommandArgs(stmt string) (string, []string) {
	// Split the statement by single whitespace
	stmtSplit := strings.Split(stmt, " ")
	// First word is the command, while the remaining (if any) are the args. Return them.
	return strings.ToUpper(stmtSplit[0]), stmtSplit[1:]
}
