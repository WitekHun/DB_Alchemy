import sqlalchemy

import csv
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, Float

# stations_read = open("clean_stations.csv", "r", newline="")
# measure_read = open("clean_measure.csv", "r", newline="")


def stations_input():
    station_ins = stations.insert()
    with open("clean_stations.csv") as stations_read:
        list = stations_read.readlines()
        list = list[1:]
        for line in list:
            line = line.rstrip()
            input = line.split(",")
            conn.execute(
                station_ins,
                [
                    {
                        "station": input[0],
                        "lattitude": input[1],
                        "longitude": input[2],
                        "elevation": input[3],
                        "name": input[4],
                        "country": input[5],
                        "state": input[6],
                    }
                ],
            )
        station_ins.compile().params


def measure_input():
    measure_ins = measure.insert()
    with open("clean_measure.csv") as measure_read:
        list = measure_read.readlines()
        list = list[1:]
        for line in list:
            line = line.rstrip()
            input = line.split(",")
            conn.execute(
                measure_ins,
                [
                    {
                        "station": input[0],
                        "date": input[1],
                        "precip": input[2],
                        "tobs": input[3],
                    }
                ],
            )
        measure_ins.compile().params


def select_station(**query):
    """
    Find stations data from stations table by given data from measure table
    """
    qs = []
    values = ()
    for k, v in query.items():
        qs.append(f"{k}=?")
        values += (v,)
        q = " AND ".join(qs)
        stat = engine.execute(f"SELECT station FROM measure WHERE {q}", values)
        stat = stat.fetchall()
        for r in stat:
            results = engine.execute("SELECT * FROM stations WHERE station = ?", r)
            for s in results:
                print(s)


def select_where(table, **query):
    """
    Select data from given table of given query
    """
    qs = []
    values = ()
    for k, v in query.items():
        qs.append(f"{k}=?")
        values += (v,)
        q = " AND ".join(qs)
        row = engine.execute(f"SELECT * FROM {table} WHERE {q}", values)
        for r in row:
            print(r)


if __name__ == "__main__":
    engine = create_engine("sqlite:///projekt.db", echo=True)
    """
    print(engine.driver)
    print(engine.table_names())
    print(engine.execute("SELECT * FROM Zadanie"))
    results = engine.execute("SELECT * FROM Zadanie")
    for r in results:
        print(r)

    print(sqlalchemy.__version__)
    """
    meta = MetaData()

    stations = Table(
        "stations",
        meta,
        Column("id", Integer, primary_key=True),
        Column("station", String),
        Column("lattitude", Float),
        Column("longitude", Float),
        Column("elevation", Float),
        Column("name", String),
        Column("country", String),
        Column("state", String),
    )

    measure = Table(
        "measure",
        meta,
        Column("id", Integer, primary_key=True),
        # Column("station_id", ForeignKey("stations.id")),
        Column("station", ForeignKey("stations.station")),
        Column("date", String),
        Column("precip", Float),
        Column("tobs", Integer),
    )

    meta.drop_all(engine, checkfirst=True)
    meta.create_all(engine, checkfirst=True)
    # stations.drop(engine, checkfirst=False)
    # stations.create(engine, checkfirst=True)
    conn = engine.connect()
    print(engine.table_names())

    # station_ins = stations.insert()
    # conn.execute(station_ins)
    # stations_read.readline()
    # input = stations_read.readline()
    # row = input.split(",")
    # list = stations_read.readlines()
    # print(list)

    stations_input()
    measure_input()
    """
    results = engine.execute(
        "SELECT * FROM Stations WHERE elevation>10 AND lattitude>21.4"
    )
    for r in results:
        print(r)
    """
    # results = conn.execute("SELECT * FROM stations LIMIT 5").fetchall()
    # for r in results:
    #    print(r)

    select_station(precip=0.08, tobs=65)
    select_where(stations, id=3)
