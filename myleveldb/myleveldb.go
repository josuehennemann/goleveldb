package myleveldb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/josuehennemann/goleveldb/leveldb"
	"github.com/josuehennemann/goleveldb/leveldb/opt"
	"github.com/josuehennemann/goleveldb/leveldb/util"
)

const NODE_CHAR_SEPARATOR = "_"

var _notFoundError = fmt.Errorf("Not found")

type LevelDB struct {
	DB *leveldb.DB
}
type Query struct {
	Page        int
	PerPage     int
	KeyContains []byte
	filterIn    string
	filterValue interface{}
	start       int
	end         int
	valueSearch []byte
}

func (q *Query) FilterIn(field string, value interface{}) {
	q.filterIn = field
	q.filterValue = value
}
func (q *Query) Contains(v []byte) bool {
	if len(q.valueSearch) == 0 {
		return true
	}
	return bytes.Contains(v, q.valueSearch)
}
func (q *Query) SetAll() {
	q.Page = -1
	q.PerPage = -1
}
func (q *Query) ContainsInKey(v []byte) bool {
	if len(q.KeyContains) == 0 {
		return true
	}

	return bytes.Contains(v, q.KeyContains)
}
func (q *Query) isAll() bool {
	return q.Page == -1 && q.PerPage == -1
}
func (q *Query) makePaginate() {
	if q.isAll() {
		return
	}
	if q.Page <= 0 {
		q.Page = 0
	}
	if q.PerPage <= 0 {
		q.PerPage = 10
	}
	q.start = q.Page * q.PerPage
	q.end = q.start + q.PerPage
}
func (q *Query) makeQuerySearch() {
	if q.filterIn == "" {
		return
	}
	value := ""
	switch v := q.filterValue.(type) {
	case string:
		value = fmt.Sprintf("\"%s", v)
	default:
		value = fmt.Sprintf("%v", v)
	}

	q.valueSearch = []byte(`"` + q.filterIn + `":` + value)
}
func OpenFile(path string, o *opt.Options) (*LevelDB, error) {
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, err
	}
	return &LevelDB{DB: db}, nil
}

func (this *LevelDB) GetDB() *leveldb.DB {
	return this.DB
}
func (this *LevelDB) Close() {
	this.DB.Close()
}

//faz get sem fazer parse, retornando exatamente os bytes salvos
func (this *LevelDB) GetRaw(key string) ([]byte, error) {
	data, err := this.DB.Get([]byte(key), nil)
	return data, err
}

//faz o get e unmarshal
func (this *LevelDB) GetParsed(key string, i interface{}) error {
	data, err := this.GetRaw(key)
	if err != nil {
		return err
	}
	err = json.Unmarshal(data, &i)
	if err != nil {
		return err
	}

	return nil
}
func (this *LevelDB) Save(idx string, value []byte) error {

	return this.DB.Put([]byte(idx), value, nil)
}

func (this *LevelDB) SaveInNode(node string, idx string, value []byte) error {
	key := BuildNodeKey(node, idx)
	return this.Save(string(key), value)
}

func (this *LevelDB) GetRawInNode(node string, key string) ([]byte, error) {
	realKey := BuildNodeKey(node, key)
	iter := this.DB.NewIterator(util.BytesPrefix(realKey), nil)
	data := []byte{}
	for iter.Next() {
		if bytes.Equal(realKey, iter.Key()) {
			data = iter.Value()
			break
		}
	}
	iter.Release()
	err := iter.Error()
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, _notFoundError
	}
	return data, nil
}

func (this *LevelDB) GetParsedInNode(node string, key string, i interface{}) error {
	data, err := this.GetRawInNode(node, key)
	if err != nil {
		return err
	}
	err = json.Unmarshal(data, &i)
	if err != nil {
		return err
	}

	return nil
}

func (this *LevelDB) SearchInNode(node string, list interface{}, q *Query) error {
	var filterRange *util.Range
	if node != "" {
		filterRange = util.BytesPrefix(BuildNodeKey(node, ""))
	}
	return this._search(filterRange, list, q)
}
func (this *LevelDB) Search(prefix string, list interface{}, q *Query) error {
	var filterRange *util.Range
	if prefix != "" {
		filterRange = util.BytesPrefix([]byte(prefix))
	}

	return this._search(filterRange, list, q)
}
func (this *LevelDB) Range(prefix string, sufix string, list interface{}, q *Query) error {
	filterRange := &util.Range{Start: []byte(prefix), Limit: []byte(sufix)}
	return this._search(filterRange, list, q)
}
func (this *LevelDB) GetAllKeysByPrefix(prefix string, sufixKey string) []string {
	iter := this.DB.NewIterator(util.BytesPrefix([]byte(prefix)), nil)
	iter.Last()
	vv := iter.Key()
	data := []string{}
	if bytes.HasSuffix(vv, []byte(sufixKey)){
		data = append(data, string(vv))
	}


	for iter.Prev() {
		vv := iter.Key()
		if bytes.HasSuffix(vv, []byte(sufixKey)){
			data = append(data, string(vv))
		}
	}
	iter.Release()
	err := iter.Error()
	if err != nil {
		return nil
	}
	return data

}
func (this *LevelDB) _search(filterRange *util.Range, list interface{}, q *Query) error {
	iter := this.DB.NewIterator(filterRange, nil)
	data := [][]byte{}

	if q != nil {
		q.makeQuerySearch()
		q.makePaginate()

	}
	count := 0
	for iter.Next() {
		vv := iter.Value()
		if q != nil {
			if !q.ContainsInKey(iter.Key()) {
				continue
			}

			if !q.Contains(vv) {
				continue
			}
			if !q.isAll() {
				if q.start > count {
					count++
					continue
				}

				if count >= q.end {
					break
				}
			}

		}
		count++

		tt := make([]byte, len(vv))
		copy(tt, vv)
		data = append(data, tt)
	}

	iter.Release()
	err := iter.Error()
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return nil
	}

	tmp := "[" + string(bytes.Join(data, []byte(","))) + "]"
	err = json.Unmarshal([]byte(tmp), &list)
	if err != nil {
		return err
	}
	return nil
}

func (this *LevelDB) Read(node string, q *Query) chan []byte {
	var filterRange *util.Range
	if node != "" {
		filterRange = util.BytesPrefix(BuildNodeKey(node, ""))
	}
	ch := make(chan []byte, 0)
	iter := this.DB.NewIterator(filterRange, nil)

	if q != nil {
		q.makeQuerySearch()
		q.makePaginate()

	}

	go func() {
		count := 0
		for iter.Next() {
			vv := iter.Value()
			if q != nil {
				if !q.ContainsInKey(iter.Key()) {
					continue
				}

				if !q.Contains(vv) {
					continue
				}
				if !q.isAll() {
					if q.start > count {
						count++
						continue
					}

					if count >= q.end {
						break
					}
				}

			}
			count++
			tt := make([]byte, len(vv))
			copy(tt, vv)

			ch <- tt
		}
		ch <- nil
		iter.Release()
		err := iter.Error()
		if err != nil {
			return
		}
	}()

	return ch
}

func BuildNodeKey(node string, idx string) []byte {
	return []byte(node + NODE_CHAR_SEPARATOR + idx)
}
