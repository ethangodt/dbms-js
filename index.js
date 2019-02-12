/**
 * https://my.bradfieldcs.com/databases/2019-02/single-table-queries/project/
 *
 * read about and use the iterator pattern
 *
 * Selection
 * FileScan
 * Projection
 *
 * Distinct
 * Sort
 *
 * not everything can just be a single row - use result instead
 *
 * you need to figure out how to make this more event based
 *
 * how can you support passing all records or just one?
 *
 */

const query = [
  ["PROJECTION", ["rating"]],
  ["SELECTION", ["title", "EQUALS", "movie4"]],
  ["FILESCAN", ["movies"]]
];

const dummyData = `id,title,rating
1,movie1,1
2,movie2,2
3,movie3,3
4,movie4,4
5,movie5,5
6,movie6,6
7,movie7,7
8,movie8,8
9,movie9,9`;

class Executor {
  constructor(query) {
    const log = row => console.log(row);
    const projector = new Projection([log], query[0][1]);
    const selector = new Selection([projector.receive], query[1][1]);
    new FileScan([selector.receive]);
  }
}

/**
 * Base "node" class.
 */
class QueryNode {
  constructor(nextList, activate) {
    this.nextList = nextList;
    this.unprocessedRecords = [];
    this.activate = activate;
    this.receive = this.receive.bind(this);
  }

  /**
   * Each node will need to accept data independently of how fast it can process
   * it (at least to support async steps in Javascript - like out of memory sort). This is
   * a dedicated method to create a queue of the incoming data streaming in.
   */
  receive(record) {
    const newTotal = this.unprocessedRecords.push(record);
    if (newTotal === 1) {
      // if there previously were no records we need to start whatever "processing"
      // this node is responsible for.
      this.activate();
    }
  }

  /**
   * This function is used to pull data off the incoming stream queue.
   */
  getNextRecord() {
    return this.unprocessedRecords.shift();
  }

  /**
   * A util to forward a process row/s to any next nodes.
   */
  forward(result) {
    this.nextList.forEach(next => next(result));
  }

  /**
   * This might not always matter, but in some case it will matter to let nodes
   * up the branch know with certainty that no more values are coming.
   * (e.g. to let a sorting node up the branch know that it has everything it needs)
   * todo - you need to pass in more functions or something
   */
  end() {}
}

class FileScan extends QueryNode {
  constructor(nextList) {
    super(nextList);
    this.currentRow = 1; // 0 is the schema
    this.schema = [];
    this.init();
  }
  init() {
    // todo base this on when you get an end of file error
    while (this.currentRow < dummyData.split(/\n/).length) {
      this.forward(this.readRow());
      this.currentRow++;
    }
  }
  readRow() {
    // todo use real files
    // todo make this lazy
    const rows = dummyData.split(/\n/);
    if (!this.schema.length) {
      // if the schema hasn't been set, set it
      this.schema = rows[0].split(",");
    }
    return rows[this.currentRow].split(",").reduce((rowAsObject, value, i) => {
      rowAsObject[this.schema[i]] = value;
      return rowAsObject;
    }, {});
  }
}

class Selection extends QueryNode {
  constructor(nextList, isolatedQuery) {
    const activate = () => this.select();
    super(nextList, activate);
    this.isolatedQuery = isolatedQuery;
  }
  select() {
    const currentRow = this.getNextRecord();
    const [header, comparison, value] = this.isolatedQuery;
    const predicate = Selection.getPredicate(comparison);
    if (predicate(currentRow[header], value)) {
      this.forward(currentRow);
    }
    if (this.unprocessedRecords.length) {
      this.select();
    }
  }
  static getPredicate(comparison) {
    switch (comparison) {
      case "EQUALS":
        return (valueToCheck, valueToCompare) =>
          valueToCheck === valueToCompare;
    }
  }
}

class Projection extends QueryNode {
  constructor(nextList, headers) {
    const activate = () => this.project();
    super(nextList, activate);
    this.headers = headers;
  }
  project() {
    const currentRow = this.getNextRecord();
    const reducedRow = Object.keys(currentRow).reduce((accumulator, key) => {
      if (this.headers.includes(key)) {
        accumulator[key] = currentRow[key];
      }
      return accumulator;
    }, {});
    this.forward(reducedRow);
    if (this.unprocessedRecords.length) {
      this.project();
    }
  }
}

new Executor(query);
