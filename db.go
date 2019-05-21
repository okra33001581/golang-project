package prd

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/astaxie/beego/orm"
	_ "github.com/go-sql-driver/mysql" // import your used driver
)

func init() {

	orm.RegisterDataBase("default", "mysql", "@tcp(192.168.100.3306)/guttv_vod?charset=utf8", 30)

	orm.RegisterModelWithPrefix("t_", new(Series), new(Product), new(ServiceGroup))
	orm.RunSyncdb("default", false, false)
}

type Mysql struct {
	sql   string
	total int64
	lock  *sync.Mutex
}

func (this *Mysql) New() {
	//this.sql = "SELECT s.*, p.code ProductCode, p.name pName  FROM guttv_vod.t_series s inner join guttv_vod.t_product p on p.itemcode=s.code  and p.isdelete=0 limit ?,?"
	this.sql = "SELECT s.*, p.code ProductCode, p.name pName  FROM guttv_vod.t_series s , guttv_vod.t_product p where p.itemcode=s.code  and p.isdelete=0 limit ?,?"
	this.total = 0
	this.lock = &sync.Mutex{}
}
func NewDB() (db *Mysql) {
	db = new(Mysql)
	db.New()
	return db
}
func (this *Mysql) GetTotal() (t int64) {
	t = 0
	this.lock.Lock()
	t = this.total
	this.lock.Unlock()
	return t
}
func (this *Mysql) toTime(toBeCharge string) int64 {
	timeLayout := "2006-01-02 15:04:05"
	loc, _ := time.LoadLocation("Local")
	theTime, _ := time.ParseInLocation(timeLayout, toBeCharge, loc)
	sr := theTime.Unix()
	if sr < 0 {
		sr = 0
	}
	return sr
}
func (this *Mysql) getSGCode(seriesCode string) (result []string, num int64) {
	sql := "select distinct ref.servicegroupcode code  from t_servicegroup_reference_category ref "
	sql = sql + "left join t_category_product cp on cp.categorycode=ref.categorycode "
	sql = sql + "left join t_package pkg on pkg.code = cp.assetcode "
	sql = sql + "left join t_package_product pp on pp.parentcode=pkg.code "
	sql = sql + "left join t_product prd on prd.code = pp.assetcode "
	sql = sql + "where   prd.itemcode=?"
	o := orm.NewOrm()
	var sg []*ServiceGroup
	num, err := o.Raw(sql, seriesCode).QueryRows(&sg)

	if err == nil {
		//fmt.Println(num)
		for _, value := range sg {
			//fmt.Println(value.Code)
			result = append(result, value.Code)
		}

	} else {
		fmt.Println(err)
	}
	//fmt.Println(result)
	return result, num
}

func (this *Mysql) formatData(value *Series) {
	//设置业务分组数据
	sg, _ := this.getSGCode(value.Code)
	//fmt.Println(sg)
	value.ServiceGroup = []string{}
	value.ServiceGroup = sg[0:]
	//更改OnlineTime为整数
	value.OnlineTimeInt = this.toTime(value.OnlineTime)
	//分解地区
	value.OriginalCountryArr = strings.Split(value.OriginalCountry, "|")
	//分解二级分类
	value.ProgramType2Arr = strings.Split(value.ProgramType2, "|")
	//写入记录内容
	value.Description = strings.Replace(value.Description, "\n", "", -1)
}
func (this *Mysql) GetData(offset int, size int, ch chan []*Series) {
	var result []*Series
	o := orm.NewOrm()
	num, err := o.Raw(this.sql, offset, size).
		QueryRows(&result)
	if err != nil {
		fmt.Println("read DB err")
		panic(err)
		//return //err, nil
	}
	for _, value := range result {
		this.formatData(value)
		//json, _ := json.Marshal(value)
		//fmt.Println(string(json))
		//fmt.Println(value.ServiceGroup)
	}
	this.lock.Lock()
	this.total += num
	this.lock.Unlock()

	fmt.Println("read count :", num) //, "Total:", Total)
	//return nil, result
	ch <- result
}
