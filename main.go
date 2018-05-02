package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"time"

	"github.com/hako/durafmt"

	"github.com/mattn/go-shellwords"
	"github.com/spf13/cobra"
	"go.uber.org/atomic"
)

var nbProc int
var queueSize int
var disableTicker bool
var c = cobra.Command{
	RunE: func(cmd *cobra.Command, args []string) error {
		return Dispatch()
	},
}

var command string

func main() {
	err := c.Execute()
	if err != nil {
		os.Exit(1)
	}
}

type Ticker struct {
	Interval time.Duration
	Start    time.Time
	events   chan bool
}

func NewTicker(interval time.Duration) *Ticker {
	ticker := new(Ticker)
	ticker.Interval = interval
	ticker.events = make(chan bool)
	ticker.Start = time.Now()
	go func() {
		for {
			ticker.events <- true
			time.Sleep(interval)
		}
	}()
	return ticker
}

func (t *Ticker) WaitTick() {
	<-t.events
	return
}

func (t *Ticker) Execute(f func(t *Ticker)) {
	go func() {
		for {
			t.WaitTick()
			f(t)
		}
	}()
}

func init() {
	c.Flags().IntVar(&nbProc, "nbproc", runtime.NumCPU(), "The number of processes you want to run in parallel, defaults to the number of this machine CPU cores")
	c.Flags().IntVar(&queueSize, "queue", 100000, "The maximum number of lines waiting to be read")
	c.Flags().StringVar(&command, "command", "", "The command to run")
	c.Flags().BoolVar(&disableTicker, "noprogress", false, "Set this flag to disable progress display")
	c.MarkFlagRequired("nbproc")
	c.MarkFlagRequired("command")
}

func Dispatch() error {
	procs := make([]*Process, nbProc)
	queue := make(chan string, queueSize)

	cmdArgs, err := shellwords.Parse(command)
	if err != nil {
		return err
	}
	cmd := cmdArgs[0]
	args := cmdArgs[1:]

	for i := 0; i < nbProc; i++ {
		proc := NewProcess(i, queue, cmd, args)
		procs[i] = proc
		proc.Start()
	}

	reader := bufio.NewReader(os.Stdin)
	var counter = atomic.NewInt64(0)
	if !disableTicker {
		NewTicker(2 * time.Second).Execute(func(ticker *Ticker) {
			elapsed := time.Now().Sub(ticker.Start)
			fmt.Printf("[---] - %v - queued %d elements\n", durafmt.Parse(elapsed.Round(1*time.Second)), counter.Load())
		})
	}

	for {
		text, err := reader.ReadString('\n')
		//End of the stream
		if err != nil {
			//Wait for all elements to be processed
			for {
				if len(queue) > 0 {
					time.Sleep(1 * time.Second)
					continue
				}
				break
			}

			//Close inputs
			for i := 0; i < nbProc; i++ {
				procs[i].Stdin.Close()
			}

			//Wait for processes to finish
			for i := 0; i < nbProc; i++ {
				procs[i].Wait()
			}
			return nil
		}
		queue <- text

		counter.Inc()
	}
}

type Process struct {
	Command string
	Args    []string
	Stdin   io.WriteCloser
	Queue   chan string
	*exec.Cmd
	ID     int
	stdout *ProcessWriter
	stderr *ProcessWriter
}

func NewProcess(id int, queue chan string, command string, args []string) *Process {
	proc := new(Process)
	proc.ID = id
	proc.Queue = queue
	proc.Command = command
	proc.Args = args
	return proc
}

//Wrap process output
func (p *ProcessWriter) Write(b []byte) (n int, err error) {
	count, err := p.buf.Write(b)
	p.buf.Flush()
	return count, err
}

type ProcessWriter struct {
	*Process
	buf *bufio.ReadWriter
}

func NewProcessWriter(p *Process) *ProcessWriter {
	reader, writer := io.Pipe()
	bufferedReader := bufio.NewReader(reader)
	bufferedWriter := bufio.NewWriter(writer)
	rw := bufio.NewReadWriter(bufferedReader, bufferedWriter)

	pWriter := new(ProcessWriter)
	pWriter.Process = p
	pWriter.buf = rw

	go func() {
		for {
			line, err := pWriter.buf.ReadString('\n')
			if err != nil {
				return
			}
			os.Stdout.WriteString(fmt.Sprintf("[%03d]%v", pWriter.Process.ID, line))
		}
	}()

	return pWriter
}

func (p *Process) Start() {
	command := exec.Command(p.Command, p.Args...)
	p.Cmd = command
	stdin, _ := command.StdinPipe()
	p.Stdin = stdin

	//stdout
	p.stdout = NewProcessWriter(p)
	command.Stdout = p.stdout
	//stderr
	p.stderr = NewProcessWriter(p)
	command.Stderr = p.stderr

	go func() {
		for {
			p.Stdin.Write([]byte(<-p.Queue))
		}
	}()

	command.Start()
}
