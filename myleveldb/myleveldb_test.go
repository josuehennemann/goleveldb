package myleveldb
// executar teste: go test -bench=. -benchmem
import "testing"
import "encoding/json"
import "time"
type Teste struct {
	A        string    `json:"a"`
	Sku      string    `json:"sku"`
	Ean      string    `json:"ean"`
	DateSync time.Time `json:"date"`
}


var testeDB *LevelDB
var q = &Query{Page: 0, PerPage: 20}

func load() {
	if testeDB != nil {
		return
	}

	q.FilterIn("a", "VALOR DO NODE NODE-1 - 9528")

	baseName := "file.db"
	db, err := OpenFile(baseName, nil)
	if err != nil {
		panic(err.Error())
	}
	testeDB = db
}
func BenchmarkSearch(b *testing.B) {
	load()

	for n := 0; n < b.N; n++ {
		list := []Teste{}
		testeDB.Search("NODE-1", &list, nil)
	}
}


func BenchmarkRead(b *testing.B) {
	load()
	for n := 0; n < b.N; n++ {
		ch := testeDB.Read("NODE-1", nil)
		for item := range ch {
			if item == nil {
				break
			}
		}
	}
}

func BenchmarkReadWithUnmarshal(b *testing.B) {
	load()
	for n := 0; n < b.N; n++ {
		list := []Teste{}
		ch := testeDB.Read("NODE-1", nil)
		for item := range ch {
			if item == nil {
				break
			}
			t := Teste{}
			json.Unmarshal(item, &t)
			list = append(list, t)
		}
	}
}

func BenchmarkSearchWithCondition(b *testing.B) {
	load()

	for n := 0; n < b.N; n++ {
		list := []Teste{}
		testeDB.Search("NODE-1", &list, q)
	}
}

func BenchmarkSearchWithConditionReverse(b *testing.B) {
	load()
	q2 := *q
	q2.Reverse = true
	for n := 0; n < b.N; n++ {
		list := []Teste{}
		testeDB.Search("NODE-1", &list, &q2)
	}
}

func BenchmarkReadWithCondition(b *testing.B) {
	load()
	for n := 0; n < b.N; n++ {
		ch := testeDB.Read("NODE-1", q)
		for item := range ch {
			if item == nil {
				break
			}
		}
	}
}

func BenchmarkReadWithUnmarshalWithCondition(b *testing.B) {
	load()
	for n := 0; n < b.N; n++ {
		list := []Teste{}
		ch := testeDB.Read("NODE-1", q)
		for item := range ch {
			if item == nil {
				break
			}
			t := Teste{}
			json.Unmarshal(item, &t)
			list = append(list, t)
		}
	}
}
