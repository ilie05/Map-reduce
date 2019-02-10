from mpi4py import MPI
from mpi_master_slave import Master, Slave
from mpi_master_slave import WorkQueue
import time
import os
import sys
import re
import json
import shutil


output_folder = "ouput_files"
temp_folder = "temp_files"
input_fodler = "input_files"


class MyApp(object):
    """
    This is my application that has a lot of work to do so it gives work to do
    to its slaves until all the work is done
    """

    def __init__(self, slaves):
        # when creating the Master we tell it what slaves it can handle
        self.master = Master(slaves)
        # WorkQueue is a convenient class that run slaves on a tasks queue
        self.work_queue = WorkQueue(self.master)

    def terminate_slaves(self):
        """
        Call this to make all slaves exit their run loop
        """
        self.master.terminate_slaves()

    def run(self, mode):
        """
        This is the core of my application, keep starting slaves
        as long as there is work to do
        """
        global output_folder
        global temp_folder
        global input_fodler

        files_number = len([name_ for name_ in os.listdir(input_fodler) if os.path.isfile(os.path.join(input_fodler, name_))])

        if os.path.isdir(input_fodler) is False:
            print("nu exista folderul cu fisierele 'input_files'")
            sys.exit()

        if mode is "mapping":
            # remove 'temp' and 'out' directory if exist
            if os.path.isdir(temp_folder):
                shutil.rmtree(temp_folder)
            if os.path.isdir(output_folder):
                shutil.rmtree(output_folder)

            # create directories again
            os.makedirs(temp_folder)
            os.makedirs(output_folder)

            #
            # let's prepare our work queue. This can be built at initialization time
            # but it can also be added later as more work become available
            #
            # in etapa de mapping masterul creaza o lista de task-uri care reprezinta numele de fisierelor de intrare
            for i in range(1, files_number + 1):
                # 'data' will be passed to the slave and can be anything
                file_name = input_fodler + "/" + str(i) + ".txt"
                self.work_queue.add_work(data=(file_name, mode))

            #
            # Keeep starting slaves as long as there is work to do
            #
            # cat timp mai sunt fisiere de prelucrat in 'work_queue' masterul distribuie un fisier oricarui slave care este liber
            while not self.work_queue.done():
                #
                # give more work to do to each idle slave (if any)
                #
                self.work_queue.do_work()

                #
                # reclaim returned data from completed slaves
                #
                # raspunurile primite de la slave
                for slave_return_data in self.work_queue.get_completed_work():
                    done, message = slave_return_data
                    if not done:
                        print(message)

        elif mode is "reduce":

            temp_files = os.listdir(temp_folder)
            # in etapa de reduce masterul creaza o lista de task-uri care reprezinta numele de fisierelor temporare create la etapa de mapare
            for file in temp_files:
                self.work_queue.add_work(data=(file, mode))

            while not self.work_queue.done():
                # cat timp mai sunt fisiere de prelucrat in 'work_queue' masterul distribuie un fisier oricarui slave care este liber
                self.work_queue.do_work()
                # raspunurile primite de la slave
                for slave_return_data in self.work_queue.get_completed_work():
                    done, message = slave_return_data
                    if not done:
                        print(message)


class MySlave(Slave):
    """
    A slave process extends Slave class, overrides the 'do_work' method
    and calls 'Slave.run'. The Master will do the rest
    """
    def __init__(self):
        super(MySlave, self).__init__()

    def do_work(self, data):
        file_name, mode = data
        if mode == "mapping":
            # mapping phase
            with open(file_name, 'rb') as file:
                temp = {}  # cream un dictionar in care tinem frecventa tuturor cuvintelor dintr-un fisier de intrare
                for line in file:
                    words = line.decode('latin-1').split()
                    for word in words:
                        word = re.sub(r'\W+', '', word).lower()
                        if word in temp:
                            temp[word] += 1
                        else:
                            temp[word] = 1
                # cream un fisiere nou pentru fiecare pereche (key: value) din dictionarul temp
                # fisierul va avea numele: numeFisierSursa_cuvant_frecventa
                for key, val in temp.items():
                    out_file_name = temp_folder + "/" + file_name.split("/")[1].split('.')[0] + "_" + key + "_" + str(val)
                    f = open(out_file_name, 'w+')
                    f.close()

            return (True, 'I completed reading  %s file' % file_name)

        elif mode == "reduce":

            word = file_name.split("_")[1]            # cuvantul care reprezinta si numelele fisierul nou creat daca nu exista sau modificat in caz ca exista
            word_location = file_name.split("_")[0]      # numele fisierului incare se gaseste cuvantul 'word'
            word_location += ".txt"
            frequency = file_name.split("_")[2]         # frecventa cu care apare cuvantul 'word' in fisierul 'word_location'

            try:
                frequency = int(frequency)
            except BaseException:
                return (False, "Exeption on frequency!!! Cannot proccess file '{}' \n".format(file_name))

            new_file_name = output_folder + "/" + word + ".txt"
            # testeaza daca fisierul cu numele new_file_name nu a fost creat
            # creaza fisierul in caz nu este creat si scrie primul obiect in el
            if not os.path.exists((new_file_name)):
                with open(new_file_name, 'w+') as file:
                    file.write(json.dumps({word_location: frequency}))
            # daca fisierul deja exista il deschide
            else:
                with open(new_file_name, 'r+')as file:
                    line = file.readline()
                    try:
                        # extarge continutul in format json (sau dict)
                        line = json.loads(line)
                        # adauga obiectul curent
                        line[word_location] = frequency
                        # scrie continutul inapoi in fisier
                        file.seek(0)
                        file.write(json.dumps(line))
                    except BaseException:
                        # caz in care doua procese scriu in acelasi fisier in acelasi timp
                        return (False, "Exeption on JSONDUMPS!!! Paralell access on file '{}' \n Line content:  {} \n".format(file_name, line))
            return (True, "JOB DONE!!")


def main():

    rank = MPI.COMM_WORLD.Get_rank()
    size = MPI.COMM_WORLD.Get_size()

    if rank == 0:  # Master
        # creaza un 'maste' cu size-1 'slaves'
        app = MyApp(slaves=range(1, size))

        start_time = time.time()
        app.run(mode="mapping")
        mapping_time = time.time() - start_time
        print("TIMP EXECUTIE MAPPING: {} \n".format(mapping_time))

        start_time = time.time()
        app.run(mode="reduce")
        reduce_time = time.time() - start_time
        print("TIMP EXECUTIE REDUCE: {} \n".format(reduce_time))

        total_time = mapping_time + reduce_time
        print("\nTIMP TOTAL DE EXECUTIE:" + str(total_time))

        app.terminate_slaves()
    else:  # Any slave
        MySlave().run()


if __name__ == "__main__":
    main()
