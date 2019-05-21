type Series struct {
	Id   int
	Name string
}

func (u *Series) TableName() string {
	return "Series"
}