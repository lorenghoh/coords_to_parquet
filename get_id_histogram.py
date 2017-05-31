from collections import defaultdict
import ujson

def main():
    filename='/scratchSSD/phil/tracking/unique_list.json'
    with open(filename, 'r') as in_f:
        tup_list = ujson.load(in_f)

    cloud_list = [cloud_id for cloud_id, timestep in tup_list]
    time_steps = [timestep for cloud_id, timestep in tup_list]

    count_dict = defaultdict(list)
    len_list = []
    for cloud_id,timestep in tup_list:
        count_dict[cloud_id].append(timestep)

    for key, value in count_dict.items():
        the_len = len(count_dict[key])
        len_list.append((key, the_len))

    def sort_len(item):
        return -item[1]

    len_list.sort(key=sort_len)

    o_dict = defaultdict(list)
    for cloud_id, _ in len_list[:30]:
        t_list = count_dict[cloud_id]
        o_dict[cloud_id] = t_list
        print(cloud_id, '%d' % len(count_dict[cloud_id]), 
              'from %3d to %3d' % (t_list[0], t_list[-1]))

    with open('unique_clouds.json', 'w') as out_f:
        ujson.dump(o_dict, out_f, sort_keys=True, indent=4)

if __name__ == '__main__':
    main()
