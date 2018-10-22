import mysql.connector
import numpy as np
import threading
import csv

class WriteToMysql:
    def __init__(self):
        self.user = 'root'
        self.pwd = '15801799809n'
        self.host = 'localhost'
        self.database = 'Mcclee'
        self.table = 'handscript '
        self.message = '(in1, prec, k, similar) '
        self.value = 'VALUES (%s, %s, %s, %s)'
        self.info = ('', '', '', '')
        self.add_data = 'insert into ' + self.table + self.message + self.value
        self.selectdata = 'select ' + self.message + 'from ' + self.table
        self.conn = mysql.connector.connect(user=self.user, password=self.pwd, host=self.host,
                                            database=self.database)
        self.conn.close()

    def writein(self, data):
        conn = mysql.connector.connect(user=self.user, password=self.pwd, host=self.host, database=self.database)
        cursor = conn.cursor()
        cursor.execute(self.add_data, data)
        conn.commit()
        conn.close()

    def selectdatas(self, sec, cond):
        selectdata = 'select ' + sec + ' from ' + self.table + cond
        conn = mysql.connector.connect(user=self.user, password=self.pwd, host=self.host, database=self.database)
        cursor = conn.cursor()
        cursor.execute(selectdata)
        return cursor.fetchall()


class NewOne:
    def __init__(self):
        self.ans = {}
        self.l = 0
        self.label = ''

    def readtrain(self, f):
        trainset = np.load(f)
        return trainset

    def readlabel(self, f):
        with open(f, 'r') as f1:
            testset = f1.read()
            return testset

    def k_nn(self, test, train, labels, k):
        self.label = self.readlabel(labels)
        test_data = self.readtrain(test)

        train_data = self.readtrain(train)
        count = 0
        for i in test_data:
            c = pow(train_data - i, 2)
            t = threading.Thread(target=self.writedata, args=(c, k, count))
            t.start()
            count += 1

    def writedata(self, c, k, count):
        q = np.sum(c, axis=1)
        index = 0
        ans = {}
        for couple in sorted(enumerate(q, 0), key=lambda x: x[1]):
            if index == k:
                break
            m = self.label[couple[0]]
            if m in ans:
                ans[m][0] += 1
                ans[m][1] += pow(couple[1], 0.5)
            else:
                ans[m] = [1, pow(couple[1], 0.5)]
            index += 1
        k_nearest_nodes = ('', 0, 0)
        for node in ans:
            if ans[node][0] > k_nearest_nodes[1] or (
                    ans[node][0] == k_nearest_nodes[1] and k_nearest_nodes[2] > ans[node][1]):
                k_nearest_nodes = (node, ans[node][0], ans[node][1])
        w = WriteToMysql()
        w.writein((count, k_nearest_nodes[0], k, k_nearest_nodes[1]))
        print(count, k_nearest_nodes[0], k, k_nearest_nodes[1])

    def savetocvs(self, data, datatile,  datafile):
        with open(datafile, 'w', newline='') as f:
            spamwriter = csv.writer(f, delimiter=',')
            spamwriter.writerow(datatile)
            for i in data:
                spamwriter.writerow((i[0]+1, i[1]))


if __name__ == '__main__':
    testpath = 'D:\ML\/test.npy'
    trainpath = 'D:\ML\/filename.npy'
    label = 'D:\ML\Output.txt'
    n = NewOne()
    # n.k_nn(testpath, trainpath, label, 9)
    w = WriteToMysql()
    res = w.selectdatas('in1, prec', '')
    n.savetocvs(res, ('ImageId', 'Label'), 'result.csv')