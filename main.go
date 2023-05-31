package main

import (
	"compress/gzip"
	"encoding/gob"
	"encoding/xml"
	"flag"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
)

var latimesPath string
var indexPath string
var counter = 0
var lexicon = make(map[string]int)
var CollectionCount = make(map[int]int)

//var metaDataChannel = make(chan DocMetaData)
//var rawTextChannel = make(chan string)
var wg sync.WaitGroup

type PostingsList [][2]int
type InvertedIndex map[int]PostingsList

var MetaData = make(map[int]DocMetaData)
var DocNo2Id = make(map[string]int)
var invertedIndex = make(InvertedIndex)

type Index struct {
	metadata      map[int]DocMetaData
	docno2id      map[string]int
	invertedIndex InvertedIndex
}

type DocXMLStruct struct {
	DOCNO    string
	DATE     DocText
	HEADLINE DocText
	TEXT     []DocText
	GRAPHIC  []DocText
}

type DocMetaData struct {
	docno    string
	docid    int
	headline string
	date     string
	length   int
}

type DocText struct {
	P []string
}

func main() {
	flag.StringVar(&latimesPath, "latimes", `C:\Users\Amir\Downloads\latimes.gz`, "Path to latimes.gz")
	flag.StringVar(&indexPath, "index", `./index`, "Where to save the indexPath")
	flag.Parse()

	if err := ReadLATimes(latimesPath, indexPath); err != nil {
		log.Fatal(err)
	}
	//wg.Wait()
	err := WriteIndex()
	if err != nil {
		log.Fatal(err)
	}
}

func WriteIndex() error {
	if err := os.MkdirAll(indexPath, 0755); err != nil {
		return err
	}
	file, err := os.Create(path.Join(indexPath, "index.gob"))
	if err != nil {
		return err
	}
	e := gob.NewEncoder(file)

	if err = e.Encode(Index{
		metadata:      MetaData,
		docno2id:      DocNo2Id,
		invertedIndex: invertedIndex,
	}); err != nil {
		return err
	}

	err = file.Close()
	if err != nil {
		return err
	}
	return nil
}

func Map[T, V any](ts []T, fn func(T) V) []V {
	result := make([]V, len(ts))
	for i, t := range ts {
		result[i] = fn(t)
	}
	return result
}

func TextSlice2String(texts []DocText) string {
	s := Map(texts, func(x DocText) string {
		return strings.Join(Map(x.P, strings.TrimSpace), "\n")
	})
	return strings.Join(s, "\n")
}

func ParseDoc(doc DocXMLStruct) (DocMetaData, string) {
	docMetaData := DocMetaData{}
	docMetaData.docno = strings.TrimSpace(doc.DOCNO)
	docMetaData.docid = counter
	counter++
	docMetaData.headline = strings.Join(Map(doc.HEADLINE.P, strings.TrimSpace), "\n")
	docMetaData.date = strings.Join(Map(doc.DATE.P, strings.TrimSpace), "\n")
	text := strings.Join([]string{
		docMetaData.headline,
		TextSlice2String(doc.TEXT),
		TextSlice2String(doc.GRAPHIC),
	}, "\n")
	return docMetaData, text
}

func UpdateCollectionCount(counts map[int]int) {
	for id, _ := range counts {
		CollectionCount[id] += 1
	}
}

func Tokenize(text string) []string {
	re := regexp.MustCompile(`\W+`)
	return re.Split(text, -1)
}

func Tokens2Ids(tokens []string, lexicon map[string]int, addToLexicon bool) []int {
	var tokenIds []int
	for _, token := range tokens {
		id, ok := lexicon[token]
		if !ok {
			if addToLexicon {
				id = len(lexicon)
				lexicon[token] = len(lexicon)
			} else {
				continue
			}
		}
		tokenIds = append(tokenIds, id)
	}
	return tokenIds

}

func CountWords(tokenIds []int) map[int]int {
	counts := make(map[int]int)
	for _, id := range tokenIds {
		counts[id] += 1
	}
	return counts
}

func AddToPostings(counts map[int]int, docId int, index InvertedIndex) {
	for tokenId, count := range counts {
		index[tokenId] = append(index[tokenId], [2]int{docId, count})
	}
}

func DocNo2Path(indexPath string, DocNo string) string {
	return filepath.Join(
		indexPath,
		"raw",
		DocNo[6:8],
		DocNo[4:6],
		DocNo[2:4],
		DocNo[len(DocNo)-4:],
	)
}

func ProcessDoc(doc DocXMLStruct) {
	docMetaData, text := ParseDoc(doc)
	tokens := Tokenize(text)
	docMetaData.length = len(tokens)
	tokenIds := Tokens2Ids(tokens, lexicon, true)
	wordCounts := CountWords(tokenIds)
	UpdateCollectionCount(wordCounts)
	AddToPostings(wordCounts, docMetaData.docid, invertedIndex)
	MetaData[docMetaData.docid] = docMetaData
	DocNo2Id[docMetaData.docno] = docMetaData.docid
	//wg.Done()
}

func ReadLATimes(latimes string, index string) error {
	fi, err := os.Open(latimes)
	if err != nil {
		return err
	}
	defer fi.Close()

	fz, err := gzip.NewReader(fi)
	if err != nil {
		return err
	}
	defer fz.Close()

	decoder := xml.NewDecoder(fz)
	for {
		tok, err := decoder.Token()
		if tok == nil || err == io.EOF {
			// EOF means we're done.
			break
		} else if err != nil {
			log.Fatalf("Error decoding token: %s", err)
		}

		switch tokenType := tok.(type) {
		case xml.StartElement:
			if tokenType.Name.Local == "DOC" {
				var docXMLStruct DocXMLStruct
				if err = decoder.DecodeElement(&docXMLStruct, &tokenType); err != nil {
					log.Fatalf("Error decoding item: %s", err)
				}
				ProcessDoc(docXMLStruct)
				//wg.Add(1)
			}
		default:
		}
	}
	return nil
}
