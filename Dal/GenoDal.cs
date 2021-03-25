using MongoDB.Bson;
using MongoDB.Driver;
using MongoLib;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace Dal
{
    public class GenoDal
    {
        public void AddGenoMongo(DataTable dt,DataTable dtMarker)
        {
            Dictionary<string, DataTable> dictSTR = new Dictionary<string, DataTable>();
            for (int i = 0; i < dtMarker.Rows.Count; i++)
            {
                DataRow dr = dtMarker.Rows[i];
                string key = dr["locus_type"].ToString();
                if (!dictSTR.ContainsKey(key))
                {
                    DataTable dttmp = new DataTable();
                    for (int a = 0; a < dr.Table.Columns.Count; a++)
                    {
                        DataColumn col = dr.Table.Columns[a];
                        dttmp.Columns.Add(col.ColumnName,col.DataType);
                    }

                    try
                    {
                        dictSTR.Add(key, dttmp);

                    }
                    catch (Exception ex)
                    {

                        throw;
                    }
                }

                try
                {
                    dictSTR[key].Rows.Add(dr.ItemArray);
                }
                catch (Exception ex)
                {

                    throw;
                }
            }

            if (dt != null && dt.Rows.Count > 0)
            {
                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    DataRow dr = dt.Rows[i];
                    BsonDocument bson = new BsonDocument();

                    #region dataread读取信息
                    var CurSTR = new BsonDocument();
                    //sample_dna_gene表id
                    string id = dr["ID"].ToString();
                    //sample_dna_gene表机构代码
                    CurSTR.Add("_id", id + ":" + dr["INIT_SERVER_NO"].ToString());
                    //sample_dna_gene表常=1，Y=2
                    string GENE_TYPE = dr["GENE_TYPE"].ToString();
                    CurSTR.Add("gene_type".ToUpper(), GENE_TYPE);

                    //person_info表对象id
                    CurSTR.Add("idlab".ToUpper(), dr["LAB_ID"].ToString());

                    //sample_dna_gene表样品id
                    CurSTR.Add("idsampling".ToUpper(), dr["SAMPLE_ID"].ToString());
                    
                    string strREAGENT_KIT = dr["REAGENT_KIT"].ToString();
                    CurSTR.Add("REAGENT_KIT", strREAGENT_KIT);

                    //sample_dna_gene表有效位点数量
                    CurSTR.Add("allele_count".ToUpper(), dr["ALLELE_COUNT"].ToString());
                    ////sample_dna_gene表位点数据
                    string GENE_INFO = dr["GENE_INFO"].ToString();
                    CurSTR.Add("GENE_INFO", GENE_INFO);

                    int index = Convert.ToInt16(dr["mongoIndex"]);
                    CurSTR.Add("mongoIndex".ToUpper(), index);
                    CurSTR.Add("DELETE", 0);
                    CurSTR.Add("ISODATE", DateTime.Now);

                    BsonDocument bdgeno = new BsonDocument();
                    int dataLength = 0;
                    string data = "";
                    #endregion

                    #region 根据GENE_TYPE填充位点名称
                    if (GENE_TYPE != "" && GENE_INFO != "")
                    {
                        string[] GENO = GENE_INFO.Split(';');
                        DataTable LocusTable = dictSTR[GENE_TYPE];
                       

                        dataLength = GENO.Length;
                        data = GENE_INFO;
                        try
                        {
                            for (int k = 0; k < GENO.Length; k++)
                            {
                                if (GENO[k] != "")
                                {
                                    string markerName = LocusTable.Select("ORD = " + (k + 1))[0]["NATIONAL_LOCUS_NAME"].ToString().ToUpper();
                                    bdgeno[markerName] = GENO[k].ToString();
                                }
                            }
                            CurSTR["GENO"] = bdgeno;
                        }
                        catch (Exception ex)
                        {

                            continue;
                        }

                        #region 写入mangoDB
                        try
                        {
                            string DB = "mongodb://{0}:{1}";
                            string IP = "192.168.2.63";
                            string PORT = "10011";
                            string LDBNAME = "LimsGene";
                            string DBCNAME = "Gene";
                            string YDBCNAME = "YGene";
                            string XDBCNAME = "XGene";

                            string connectionString = string.Format(DB, IP, PORT);
                            string collectionName = DBCNAME;
                            if (GENE_TYPE.Equals("2")) collectionName = YDBCNAME;
                            else if (GENE_TYPE.Equals("3")) collectionName = XDBCNAME;

                            IMongoDatabase mongobase = MongoDBHelper.creatMongoConnection(connectionString, LDBNAME);
                            var collection = mongobase.GetCollection<BsonDocument>(collectionName);
                            collection.InsertOne(CurSTR);

                        }
                        catch (Exception ex)
                        {
                           
                            continue;
                        }
                        #endregion
                    }
                    #endregion

                }
            }
        }

        



    }
}
