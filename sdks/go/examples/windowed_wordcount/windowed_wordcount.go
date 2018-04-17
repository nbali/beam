// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"flag"
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"
	"context"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	"time"
	"math/rand"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/window"
	"fmt"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/go/examples/windowed_wordcount/wordcount"
)

var (
	// By default, this example reads from a public dataset containing the text of
	// King Lear. Set this option to choose a different input file or glob.
	input = flag.String("input", "gs://apache-beam-samples/shakespeare/kinglear.txt", "File(s) to read.")

	// Set this required option to specify where to write the output file.
	output = flag.String("output", "", "Output (required).")
)

// Concept #2: A DoFn that sets the data element timestamp. This is a silly method, just for
// this example, for the bounded data case.
//
// Imagine that many ghosts of Shakespeare are all typing madly at the same time to recreate
// his masterworks. Each line of the corpus will get a random associated timestamp somewhere in a
// 2-hour period.

type addTimestampFn struct {
	Min beam.EventTime `json:"min"`
}

func (f *addTimestampFn) ProcessElement(x beam.X) (beam.EventTime, beam.X) {
	timestamp := f.Min.Add(time.Duration(rand.Int63n(2 * time.Hour.Nanoseconds())))
	return timestamp, x
}

// formatFn is a DoFn that formats a windowed word and its count as a string.
func formatFn(iw beam.Window, et beam.EventTime, w string, c int) string {
	s:= fmt.Sprintf("%v@%v %s: %v", et, iw, w, c)
	return s
}

func main() {
	flag.Parse()
	beam.Init()

	ctx := context.Background()

	if *output == "" {
		log.Exit(ctx, "No output provided")
	}

	p := beam.NewPipeline()
	s := p.Root()

	// Concept #1: the Beam SDK lets us run the same pipeline with either a bounded or
	// unbounded input source.
	lines := textio.Read(s, *input)

	// Concept #2: Add an element timestamp, using an artificial time just to show windowing.
	timestampedLines := beam.ParDo(s, &addTimestampFn{Min: mtime.Now()}, lines)

	// Concept #3: WindowingStrategy into fixed windows. The fixed window size for this example is 1
	// minute. See the documentation for more information on how fixed windows work, and
	// for information on the other types of windowing available (e.g., sliding windows).
	windowedLines := beam.WindowInto(s, window.NewFixedWindows(time.Minute), timestampedLines)

	// Concept #4: Re-use our existing CountWords transform that does not have knowledge of
	// windows over a PCollection containing windowed values.
	counted := wordcount.CountWords(s, windowedLines)

	// TODO(herohde) 4/16/2018: textio.Write does not support windowed writes, so we
	// simply include the window in the output and re-window back into the global window
	// before the write.

	formatted := beam.ParDo(s, formatFn, counted)
	merged := beam.WindowInto(s, window.NewGlobalWindows(), formatted)
	textio.Write(s, *output, merged)

	if err := beamx.Run(context.Background(), p); err != nil {
		log.Exitf(ctx, "Failed to execute job: %v", err)
	}
}
