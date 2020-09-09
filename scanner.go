package main

import (
	"bufio"
	"fmt"
	"log"
	"math"
	"os"
	"runtime"
	"time"
	"github.com/piquette/finance-go/chart"
	"github.com/piquette/finance-go/datetime"
)

type data struct {
	Ticker string
	Timestamp int
	Volume int
}

func getTickers() []string {
	tickers := []string{}

	file, err := os.Open("./tickers.txt")
    if err != nil {
        log.Fatal(err)
    }
    defer file.Close()

    scanner := bufio.NewScanner(file)
    for scanner.Scan() {
        tickers = append(tickers, scanner.Text())
    }

    if err := scanner.Err(); err != nil {
        log.Fatal(err)
	}

	return tickers
}

func getData(ticker string, start *time.Time, end *time.Time) []data {
	params := &chart.Params {
		Symbol: ticker,
		Start: datetime.New(start),
		End: datetime.New(end),
		Interval: datetime.OneDay,
	}

	iter := chart.Get(params)

	d := []data{}

	for iter.Next() {
		d = append(d, data{iter.Meta().Symbol, iter.Bar().Timestamp, iter.Bar().Volume})
	}
	if err := iter.Err(); err != nil {
		fmt.Println(ticker, err)
	}

	return d
}

func getMeanAndStdDev(slice []data) (float64, float64) {
	sum := 0

	for _, v := range slice {
		sum += v.Volume
	}

	mean := float64(sum) / float64(len(slice))

	sumOfSquares := 0.0

	for _, v := range slice {
		sumOfSquares += math.Pow((float64(v.Volume) - mean), 2.0)
	}

	stdDev := math.Sqrt(sumOfSquares / float64(len(slice)))

	return mean, stdDev
}

func worker(id int, start *time.Time, end *time.Time, tickers <-chan string, results chan<- data) {
	for t := range tickers {
		d := getData(t, start, end)

		if len(d) == 0 {
			results <- data{"", 0, 0}
			continue;
		}

		mean, stdDev := getMeanAndStdDev(d)
		cutoff := int(mean + stdDev * 9.0) // 9 std devs away

		done := false

		for i := len(d) - 1; i >= 0; i-- {
			date := time.Unix(int64(d[i].Timestamp), 0)
			days := int(end.Sub(date).Hours() / 24)

			if (days >= 3) {
				results <- data{"", 0, 0}
				done = true;
				break
			}

			if d[i].Volume >= int(cutoff) { // 3 days matter
				results <- d[i]
				done = true;
				break;
			}
		}

		if !done {
			results <- data{"", 0, 0}
		}
    }
}

func main() {
	workers := runtime.NumCPU()
	start := time.Now().AddDate(0, -5, 0) // 5 months of data
	end := time.Now()

	tickers := getTickers()

	tickersChan := make(chan string, len(tickers))
	resultsChan := make(chan data, len(tickers))

	for w := 1; w < workers; w++ {
		go worker(w, &start, &end, tickersChan, resultsChan)
	}

	for _, v := range tickers {
		tickersChan <- v
	}

	close(tickersChan)

	worker(0, &start, &end, tickersChan, resultsChan)

	results := []data{}

	for i := 0; i < len(tickers); i++ {
		d := <-resultsChan

		if d.Ticker != "" {
			results = append(results, d)
		}
	}

	for _, r := range results {
		fmt.Printf("%s - Volume: %d\n", r.Ticker, r.Volume)
	}
}
