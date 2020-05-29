import bisect
import copy
import operator

from ..common import find_att, string_to_datetime


def check_for_dupes(final_match, final_track, res):
    for obj in res:
        found = 0
        pos = bisect.bisect_left(final_track, obj["id"])
        if not final_match or pos > len(final_track) - 1 or final_track[pos] != obj["id"]:
            final_track.insert(pos, obj["id"])
            final_match.insert(pos, obj)
        else:
            obj_time = find_att(obj)
            while pos != len(final_track) and obj["id"] == final_track[pos]:
                if find_att(final_match[pos]) == obj_time:
                    found = 1
                    break
                else:
                    pos = pos + 1
            if found == 1:
                continue
            else:
                final_track.insert(pos, obj["id"])
                final_match.insert(pos, obj)


def check_version(data, relate):
    id_track = []
    res = []
    for obj in data:
        pos = bisect.bisect_left(id_track, obj["id"])
        if not res or pos >= len(id_track) or id_track[pos] != obj["id"]:
            id_track.insert(pos, obj["id"])
            res.insert(pos, obj)
        else:
            if relate(find_att(obj), find_att(res[pos])):
                res[pos] = obj
    return res


class BasicFilter(object):

    def __init__(self, filter_args):
        self.filter_args = filter_args

    def sort_and_paginate(self, data, limit, manifest):
        temp = None
        next_save = {}
        headers = {}
        new = []
        if len(data) == 0:
            return new, next_save, headers
        if manifest:
            """
            manifest.sort(key=lambda x: x['date_added'])
            for man in manifest:
                man_time = find_att(man)
                for check in data:
                    check_time = find_att(check)
                    if check['id'] == man['id'] and check_time == man_time:
                        if len(headers) == 0:
                            headers["X-TAXII-Date-Added-First"] = man["date_added"]
                        new.append(check)
                        temp = man
                        if len(new) == limit:
                            headers["X-TAXII-Date-Added-Last"] = man["date_added"]
                        break
            if limit and limit < len(data):
                next_save = new[limit:]
                new = new[:limit]
            else:
                headers["X-TAXII-Date-Added-Last"] = temp["date_added"]
        """
            data.sort(key=lambda x: x['_manifest_pointer']['date_added'])
            if limit and limit < len(data):
                next_save = data[limit:]
                data = data[:limit]
            headers["X-TAXII-Date-Added-First"] = data[0]['_manifest_pointer']["date_added"]
            headers["X-TAXII-Date-Added-Last"] = data[-1]['_manifest_pointer']["date_added"]
            new = data
        else:
            data.sort(key=lambda x: x['date_added'])
            if limit and limit < len(data):
                next_save = data[limit:]
                data = data[:limit]
            headers["X-TAXII-Date-Added-First"] = data[0]["date_added"]
            headers["X-TAXII-Date-Added-Last"] = data[-1]["date_added"]
            new = data
        return new, next_save, headers

    @staticmethod
    def filter_by_id(data, id_):
        id_ = id_.split(",")

        match_objects = []

        for obj in data:
            if "id" in obj and any(s == obj["id"] for s in id_):
                match_objects.append(obj)

        return match_objects

    @staticmethod
    def filter_by_added_after(data, manifest_info, added_after_date):
        added_after_timestamp = string_to_datetime(added_after_date)
        new_results = []
        if manifest_info is None:
            for obj in data:
                if string_to_datetime(obj["date_added"]) > added_after_timestamp:
                    new_results.append(obj)
        # for other objects with manifests
        else:
            for obj in data:
                if string_to_datetime(obj["_manifest_pointer"]["date_added"]) > added_after_timestamp:
                    new_results.append(obj)
        return new_results
        
        """added_after_timestamp = string_to_datetime(added_after_date)
        new_results = []
        # for manifest objects and versions
        if manifest_info is None:
            for obj in data:
                if string_to_datetime(obj["date_added"]) > added_after_timestamp:
                    new_results.append(obj)
        # for other objects with manifests
        else:
            for obj in data:
                obj_time = find_att(obj)
                for item in manifest_info:
                    item_time = find_att(item)
                    if item["id"] == obj["id"] and item_time == obj_time and string_to_datetime(item["date_added"]) > added_after_timestamp:
                        new_results.append(obj)
                        break
        return new_results
        """

    @staticmethod
    def filter_by_version(data, version):
        # final_match is a sorted list of objects
        final_match = []
        # final_track is a sorted list of id's
        final_track = []
        mid_match = []

        # return most recent object versions unless otherwise specified
        if version is None:
            version = "last"
        version_indicators = version.split(",")

        if "all" in version_indicators:
            # if "all" is in the list, just return everything
            return data

        data.sort(key=lambda x: x['id'])
        
        actual_dates = [string_to_datetime(x) for x in version_indicators if x != "first" and x != "last"]
        # if a specific version is given, filter for objects with that value
        if actual_dates:
            for obj in data:
                if any(find_att(obj) == date for date in actual_dates):
                    mid_match.append(obj)
            """
            id_track = []
            res = []
            for obj in data:
                obj_time = find_att(obj)
                if obj_time in actual_dates:
                    pos = bisect.bisect_left(id_track, obj["id"])
                    id_track.insert(pos, obj["id"])
                    res.insert(pos, obj)
            final_match = res
            final_track = id_track
            """

        if "first" in version_indicators:
            pass
            #res = check_version(data, operator.lt)
            #check_for_dupes(final_match, final_track, res)

        if "last" in version_indicators:
            same = []
            x = 0
            for x in range(0, len(data)):
                if not same or same[0]['id'] == data[x]['id']:
                    same.append(data[x])
                else:
                    same.sort(key=lambda z: find_att(z))
                    mid_match.append(same[-1])
                    same = []
                    same.append(data[x])
            else:
                same.sort(key=lambda z: find_att(z))
                mid_match.append(same[-1])
            #res = check_version(data, operator.gt)
            #check_for_dupes(final_match, final_track, res)

        mid_match.sort(key=lambda z: z['id']))

        same = []
        x = 0
        for x in range(0, len(mid_match)):
            if not same or same[0]['id'] == mid_match[x]['id']:
                same.append(data[x])
            else:
                same.sort(key=lambda z: find_att(z))
                y = 0
                for y in range(0, len(same)):
                    if same[y]['version'] != same[y+1]['version']:
                        final_match.append(same[y])
                same = []
                same.append(mid_match[x])
        else:
            same.sort(key=lambda z: find_att(z))
            mid_match.append(same[-1])
        
        return final_match

    @staticmethod
    def filter_by_type(data, type_):
        type_ = type_.split(",")
        match_objects = []

        for obj in data:
            if "type" in obj and any(s == obj["type"] for s in type_):
                match_objects.append(obj)
            elif "id" in obj and any(s == obj["id"].split("--")[0] for s in type_):
                match_objects.append(obj)

        return match_objects

    @staticmethod
    def filter_by_spec_version(data, spec_):
        spec_ = spec_.split(",")

        match_objects = []

        for obj in data:
            if "spec_version" in obj and any(s == obj["spec_version"] for s in spec_):
                match_objects.append(obj)
            elif "media_type" in obj and any(s == obj["media_type"].split("version=")[1] for s in spec_):
                match_objects.append(obj)

        return match_objects

    def process_filter(self, data, allowed, manifest_info, limit):
        filtered_by_type = []
        filtered_by_id = []
        filtered_by_spec_version = []
        filtered_by_added_after = []
        filtered_by_version = []
        final_match = []
        save_next = []
        headers = {}

        # match for type and id filters first
        match_type = self.filter_args.get("match[type]")
        if match_type and "type" in allowed:
            filtered_by_type = self.filter_by_type(data, match_type)
        else:
            filtered_by_type = copy.deepcopy(data)

        match_id = self.filter_args.get("match[id]")
        if match_id and "id" in allowed:
            filtered_by_id = self.filter_by_id(filtered_by_type, match_id)
        else:
            filtered_by_id = filtered_by_type

        # match for spec_version
        match_spec_version = self.filter_args.get("match[spec_version]")
        if match_spec_version and "spec_version" in allowed:
            filtered_by_spec_version = self.filter_by_spec_version(filtered_by_id, match_spec_version)
        else:
            filtered_by_spec_version = filtered_by_id

        # match for added_after
        added_after_date = self.filter_args.get("added_after")
        if added_after_date:
            filtered_by_added_after = self.filter_by_added_after(filtered_by_spec_version, manifest_info, added_after_date)
        else:
            filtered_by_added_after = filtered_by_spec_version

        # match for version, and get rid of duplicates as appropriate
        if "version" in allowed:
            match_version = self.filter_args.get("match[version]")
            filtered_by_version = self.filter_by_version(filtered_by_added_after, match_version)
        else:
            filtered_by_version = filtered_by_added_after

        # sort objects by date_added of manifest and paginate as necessary
        final_match, save_next, headers = self.sort_and_paginate(filtered_by_version, limit, manifest_info)

        return final_match, save_next, headers
