package main

import (
  "encoding/csv"
  "fmt"
  "os"
  "strconv"
  "time"
)

func main() {
  start := time.Now()
  jobs := extractJobs()
  expenses := extractExpenses()
  jobs = obtainCostPerJob(jobs, expenses) //transform
  load(jobs)
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
  Reason string // Labor, miscellaneous, legal, licenses
}

func extractJobs() []*Job {
  result := []*Job{}
  f, _ := os.Open("./Jobs.txt")
  defer f.Close()
  r := csv.NewReader(f)

  for record, err := r.Read(); err == nil; record, err = r.Read(){
    job := new(Job)
    job.JobId, _ = strconv.Atoi(record[0])
    job.DepartmentId, _ = strconv.Atoi(record[1])
    job.JobType = record[2]
    result = append(result, job)
  }
  return result
}

func extractExpenses() map[int][]*Expense {
  f, _ := os.Open("./Expenses.txt")
  defer f.Close()
  r := csv.NewReader(f)

  records, _ := r.ReadAll()
  expenseByJobIdCache := make(map[int][]*Expense)
  for _, record := range records {
    expense := new(Expense)
    expense.ExpenseId, _ = strconv.Atoi(record[0])
    expense.JobId, _ = strconv.Atoi(record[1])
    expense.Price, _ = strconv.ParseFloat(record[2], 64)
    expense.Reason = record[3]

    expenseByJobIdCache[expense.JobId] = append(expenseByJobIdCache[expense.JobId], expense)
  }
  return expenseByJobIdCache
}

func obtainCostPerJob(jobs []*Job, expensesForJobIdCache map[int][]*Expense) []*Job {
  for _, job := range jobs {
    expensesForJobId := expensesForJobIdCache[job.JobId]
    sum := 0.0
    for _, expense := range expensesForJobId {
      sum = sum + expense.Price
    }
    job.TotalCost = sum
  }
  return jobs
}


func load(jobs []*Job) {
  f, _ := os.Create("./ExpenseReport.txt")
  defer f.Close()

  fmt.Fprintf(f, "%12s%20s%15s%15s\n",
    "Job Id", "Department Id", "Job Type", "Total Cost")

  for _, job := range jobs {
    fmt.Fprintf(f, "%12d%20d%15s%15.2f\n",
      job.JobId, job.DepartmentId, job.JobType, job.TotalCost)
  }
}
