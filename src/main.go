package main

import (
  "encoding/csv"
  "fmt"
  "os"
  "strconv"
  "time"
  "runtime"
)

func main() {
  runtime.GOMAXPROCS(4)

  start := time.Now()

  extractChannel := make(chan *Job)
  transformChannel := make(chan *Job)
  doneChannel := make(chan bool)

  go extractJobs(extractChannel)
  go obtainCostPerJob(extractChannel, transformChannel) //transform
  go load(transformChannel, doneChannel)

  <- doneChannel
  fmt.Println(time.Since(start))

}

type Job struct {
  JobId int
  DepartmentId int //[1-7]
  JobType string //[finance, software, accounting, operations]

  TotalCost float64
}

type Expense struct {
  ExpenseId int
  JobId int
  Price float64
  Reason string // labor, miscellaneous, legal, licenses
}

func extractJobs(ch chan *Job) {
  f, _ := os.Open("./Jobs.txt")
  defer f.Close()
  r := csv.NewReader(f)

  for record, err := r.Read(); err == nil; record, err = r.Read(){
    job := new(Job)
    job.JobId, _ = strconv.Atoi(record[0])
    job.DepartmentId, _ = strconv.Atoi(record[1])
    job.JobType = record[2]
    ch <- job
  }
  close(ch)
}

func obtainCostPerJob(extractChannel, transformChannel chan *Job) {
  f, _ := os.Open("./Expenses.txt")
  defer f.Close()
  r := csv.NewReader(f)

  records, _ := r.ReadAll()
  expensesForJobIdCache := make(map[int][]*Expense)
  for _, record := range records {
    expense := new(Expense)
    expense.ExpenseId, _ = strconv.Atoi(record[0])
    expense.JobId, _ = strconv.Atoi(record[1])
    expense.Price, _ = strconv.ParseFloat(record[2], 64)
    expense.Reason = record[3]
    expensesForJobIdCache[expense.JobId] = append(expensesForJobIdCache[expense.JobId], expense)
  }

  numMessages := 0
  for job := range extractChannel {
    numMessages++
    go func(job *Job) {
      expensesForJobId := expensesForJobIdCache[job.JobId]
      sum := 0.0
      for _, expense := range expensesForJobId {
        sum = sum + expense.Price
      }
      job.TotalCost = sum
      transformChannel <- job
      numMessages--
    }(job)
  }
  for ;numMessages > 0; {
    time.Sleep(1 * time.Millisecond)
  }
  close(transformChannel)
}


func load(transformChannel chan *Job, doneChannel chan bool) {
  f, _ := os.Create("./ExpenseReport.txt")
  defer f.Close()

  fmt.Fprintf(f, "%12s%20s%15s%15s\n",
    "Job Id", "Department Id", "Job Type", "Total Cost")

  numMessages := 0

  for job := range transformChannel {
    numMessages++
    go func(job *Job) {
      fmt.Fprintf(f, "%12d%20d%15s%15.2f\n",
        job.JobId, job.DepartmentId, job.JobType, job.TotalCost)
      numMessages--
    }(job)
  }
  for ;numMessages > 0; {
    time.Sleep(1 * time.Millisecond)
  }
  doneChannel <- true
}
