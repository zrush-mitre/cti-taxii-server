import bisect
import copy
import operator

from ..common import determine_spec_version, find_att, string_to_datetime


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

    @staticmethod
    def filter_by_version(data, version):
        # final_match is a sorted list of objects
        final_match = []
        # final_track is a sorted list of id's
        final_track = []

        # return most recent object versions unless otherwise specified
        if version is None:
            version = "last"
        version_indicators = version.split(",")

        if "all" in version_indicators:
            # if "all" is in the list, just return everything
            return data

        actual_dates = [string_to_datetime(x) for x in version_indicators if x != "first" and x != "last"]
        # if a specific version is given, filter for objects with that value
        if actual_dates:
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

        if "first" in version_indicators:
            res = check_version(data, operator.lt)
            check_for_dupes(final_match, final_track, res)

        if "last" in version_indicators:
            res = check_version(data, operator.gt)
            check_for_dupes(final_match, final_track, res)

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
        match_objects = []

        if spec_:
            spec_ = spec_.split(",")
            for obj in data:
                if "media_type" in obj:
                    if any(s == obj["media_type"].split("version=")[1] for s in spec_):
                        match_objects.append(obj)
                elif any(s == determine_spec_version(obj) for s in spec_):
                    match_objects.append(obj)
        else:
            for obj in data:
                add = True
                if "media_type" in obj:
                    s1 = obj["media_type"].split("version=")[1]
                else:
                    s1 = determine_spec_version(obj)
                for match in data:
                    if "media_type" in match:
                        s2 = match["media_type"].split("version=")[1]
                    else:
                        s2 = determine_spec_version(match)
                    if obj["id"] == match["id"] and s2 > s1:
                        add = False
                if add:
                    match_objects.append(obj)
        return match_objects

    @staticmethod
    def filter_by_anything(data, filter_, subject):
        tlps = {
            "white": "marking-definition--613f2e26-407d-48c7-9eca-b8e91df99dc9",
            "green": "marking-definition--34098fce-860f-48ae-8e50-ebd3cc5e41da",
            "amber": "marking-definition--f88d31f6-486f-44da-b317-01333bde0b82",
            "red": "marking-definition--5e57c739-391a-4eb3-b6be-7d15ca92d5ed"
        }
        filter_ = filter_.split(",")
        match_objects = []

        for obj in data:
            if subject in obj:
                if type(obj[subject]) is str:
                    if any(f == obj[subject] for f in filter_):
                        match_objects.append(obj)
                elif type(obj[subject]) is list:
                    if any(any(f == x for x in obj[subject]) for f in filter_):
                        match_objects.append(obj)
                elif type(obj[subject]) is int:
                    if any(int(f) == obj[subject] for f in filter_):
                        match_objects.append(obj)
            elif subject == "tlp" and "object_marking_refs" in obj:
                if any(tlps[f] in obj["object_marking_refs"] for f in filter_):
                    match_objects.append(obj)
            elif "external_references" in obj:
                for e in obj["external_references"]:
                    if subject in e:
                        if any(f == e[subject] for f in filter_):
                            match_objects.append(obj)

        return match_objects

    def process_filter(self, data, allowed=(), manifest_info=(), limit=None):
        # ais filters
        ais_filters = ["source_ref", "target_ref", "relationship_type",
                       "sighting_of_ref", "object_marking_refs", "tlp",
                       "external_id", "source_name", "created_by_ref",
                       "confidence", "sectors", "labels", "object_refs",
                       "value"]

        filtered_by_type = []
        filtered_by_id = []
        filtered_by_spec_version = []
        filtered_by_added_after = []
        filtered_by_version = []
        final_match = []
        save_next = []
        headers = {}

        # ais proposed filters
        filtered_by_ais_filter = copy.deepcopy(data)
        if "ais" in allowed:
            for fil in ais_filters:
                match_ais = self.filter_args.get("match[" + fil + "]")
                if match_ais is not None:
                    filtered_by_ais_filter = self.filter_by_anything(filtered_by_ais_filter, match_ais, fil)

        # match for type and id filters first
        match_type = self.filter_args.get("match[type]")
        if match_type and "type" in allowed:
            filtered_by_type = self.filter_by_type(filtered_by_ais_filter, match_type)
        else:
            filtered_by_type = filtered_by_ais_filter

        match_id = self.filter_args.get("match[id]")
        if match_id and "id" in allowed:
            filtered_by_id = self.filter_by_id(filtered_by_type, match_id)
        else:
            filtered_by_id = filtered_by_type

        # match for added_after
        added_after_date = self.filter_args.get("added_after")
        if added_after_date:
            filtered_by_added_after = self.filter_by_added_after(filtered_by_id, manifest_info, added_after_date)
        else:
            filtered_by_added_after = filtered_by_id

        # match for spec_version
        match_spec_version = self.filter_args.get("match[spec_version]")
        if "spec_version" in allowed:
            filtered_by_spec_version = self.filter_by_spec_version(filtered_by_added_after, match_spec_version)
        else:
            filtered_by_spec_version = filtered_by_added_after

        # match for version, and get rid of duplicates as appropriate
        if "version" in allowed:
            match_version = self.filter_args.get("match[version]")
            filtered_by_version = self.filter_by_version(filtered_by_spec_version, match_version)
        else:
            filtered_by_version = filtered_by_spec_version

        # sort objects by date_added of manifest and paginate as necessary
        final_match, save_next, headers = self.sort_and_paginate(filtered_by_version, limit, manifest_info)

        return final_match, save_next, headers
