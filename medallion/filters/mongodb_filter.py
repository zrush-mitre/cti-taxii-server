import pymongo

from ..common import datetime_to_float, string_to_datetime
from ..exceptions import ProcessingError
from .basic_filter import BasicFilter


class MongoDBFilter(BasicFilter):

    def __init__(self, filter_args, basic_filter, allowed, record=None):
        super(MongoDBFilter, self).__init__(filter_args)
        self.basic_filter = basic_filter
        self.full_query = self._query_parameters(allowed)
        self.record = record

    def _query_parameters(self, allowed):
        parameters = self.basic_filter
        if self.filter_args:

            # proposed filters
            tlps = {
                "white": "marking-definition--613f2e26-407d-48c7-9eca-b8e91df99dc9",
                "green": "marking-definition--34098fce-860f-48ae-8e50-ebd3cc5e41da",
                "amber": "marking-definition--f88d31f6-486f-44da-b317-01333bde0b82",
                "red": "marking-definition--5e57c739-391a-4eb3-b6be-7d15ca92d5ed"
            }

            if "ais" in allowed:
                match_source_ref = self.filter_args.get("match[source_ref]")
                if match_source_ref:
                    source_refs_ = match_source_ref.split(",")
                    if len(source_refs_) == 1:
                        parameters["source_ref"] = {"$eq": source_refs_[0]}
                    else:
                        parameters["source_ref"] = {"$in": source_refs_}
                match_target_ref = self.filter_args.get("match[target_ref]")
                if match_target_ref:
                    target_refs_ = match_target_ref.split(",")
                    if len(target_refs_) == 1:
                        parameters["target_ref"] = {"$eq": target_refs_[0]}
                    else:
                        parameters["target_ref"] = {"$in": target_refs_}
                match_relationship_type = self.filter_args.get("match[relationship_type]")
                if match_relationship_type:
                    relationship_types_ = match_relationship_type.split(",")
                    if len(relationship_types_) == 1:
                        parameters["relationship_type"] = {"$eq": relationship_types_[0]}
                    else:
                        parameters["relationship_type"] = {"$in": relationship_types_}
                match_sighting_of_refs = self.filter_args.get("match[sighting_of_ref]")
                if match_sighting_of_refs:
                    sighting_of_refs_ = match_sighting_of_refs.split(",")
                    if len(sighting_of_refs_) == 1:
                        parameters["sighting_of_ref"] = {"$eq": sighting_of_refs_[0]}
                    else:
                        parameters["sighting_of_ref"] = {"$in": sighting_of_refs_}
                match_object_marking_refs = self.filter_args.get("match[object_marking_refs]")
                if match_object_marking_refs:
                    object_marking_refs_ = match_object_marking_refs.split(",")
                    parameters["object_marking_refs"] = {"$in": object_marking_refs_}
                #this needs to check object_marking_refs, not id
                match_tlp = self.filter_args.get("match[tlp]")
                if match_tlp:
                    tlp_ids_ = []
                    tlps_ = match_tlp.split(",")
                    for t in tlps_:
                        if t in tlps:
                            tlp_ids_.append(tlps[t])
                        else:
                            raise ProcessingError("The server did not understand the request or filter parameters: 'tlp' value not a valid tlp marking", 400)
                    parameters["object_marking_refs"] = {"$in": tlp_ids_}
                match_external_id = self.filter_args.get("match[external_id]")
                if match_external_id:
                    external_ids_ = match_external_id.split(",")
                    if len(external_ids_) == 1:
                        parameters["external_references.external_id"] = {"$eq": external_ids_[0]}
                    else:
                        parameters["external_references.external_id"] = {"$in": external_ids_}
                match_source_name = self.filter_args.get("match[source_name]")
                if match_source_name:
                    source_names_ = match_source_name.split(",")
                    if len(source_names_) == 1:
                        parameters["external_references.source_name"] = {"$eq": source_names_[0]}
                    else:
                        parameters["external_references.source_name"] = {"$in": source_names_}
                match_created_by_ref = self.filter_args.get("match[created_by_ref]")
                if match_created_by_ref:
                    created_by_refs_ = match_created_by_ref.split(",")
                    if len(created_by_refs_) == 1:
                        parameters["created_by_ref"] = {"$eq": created_by_refs_[0]}
                    else:
                        parameters["created_by_ref"] = {"$in": created_by_refs_}
                match_confidence = self.filter_args.get("match[confidence]")
                if match_confidence:
                    confidences_ = match_confidence.split(",")
                    if len(confidences_) == 1:
                        parameters["confidence"] = {"$eq": int(confidences_[0])}
                    else:
                        int_confidences_ = [int(x) for x in confidences_]
                        parameters["confidence"] = {"$in": int_confidences_}
                match_sectors = self.filter_args.get("match[sectors]")
                if match_sectors:
                    sectors_ = match_sectors.split(",")
                    parameters["sectors"] = {"$in": sectors_}
                match_labels = self.filter_args.get("match[labels]")
                if match_labels:
                    labels_ = match_labels.split(",")
                    parameters["labels"] = {"$in": labels_}
                match_object_refs = self.filter_args.get("match[object_refs]")
                if match_object_refs:
                    object_refs_ = match_object_refs.split(",")
                    parameters["object_refs"] = {"$in": object_refs_}
                match_opinion = self.filter_args.get("match[opinion]")
                if match_opinion:
                    opinions_ = match_opinion.split(",")
                    if len(opinions_) == 1:
                        parameters["opinion"] = {"$eq": opinions_[0]}
                    else:
                        parameters["opinion"] = {"$in": opinions_}
                match_value = self.filter_args.get("match[value]")
                if match_value:
                    values_ = match_value.split(",")
                    if len(values_) == 1:
                        parameters["value"] = {"$eq": values_[0]}
                    else:
                        parameters["value"] = {"$in": values_}
                match_valid_on_after = self.filter_args.get("match[valid_on_after]")
                if match_valid_on_after:
                    valid_on_after_ = match_valid_on_after.split(",")
                    if len(valid_on_after_) == 1:
                        parameters["valid_from"] = {"$lte": datetime_to_float(string_to_datetime(valid_on_after_[0]))}
                        parameters["revoked"] = {"$exists": false}
                    else:
                        parameters["valid_from"] = {"$in": valid_on_after_}
                # end of ais filters

            match_type = self.filter_args.get("match[type]")
            if match_type and "type" in allowed:
                types_ = match_type.split(",")
                if len(types_) == 1:
                    parameters["type"] = {"$eq": types_[0]}
                else:
                    parameters["type"] = {"$in": types_}
            match_id = self.filter_args.get("match[id]")
            if match_id and "id" in allowed:
                ids_ = match_id.split(",")
                if len(ids_) == 1:
                    parameters["id"] = {"$eq": ids_[0]}
                else:
                    parameters["id"] = {"$in": ids_}
            match_spec_version = self.filter_args.get("match[spec_version]")
            if match_spec_version and "spec_version" in allowed:
                spec_versions = match_spec_version.split(",")
                media_fmt = "application/stix+json;version={}"
                if len(spec_versions) == 1:
                    parameters["_manifest.media_type"] = {
                        "$eq": media_fmt.format(spec_versions[0])
                    }
                else:
                    parameters["_manifest.media_type"] = {
                        "$in": [media_fmt.format(x) for x in spec_versions]
                    }
            added_after_date = self.filter_args.get("added_after")
            if added_after_date:
                added_after_timestamp = datetime_to_float(string_to_datetime(added_after_date))
                parameters["_manifest.date_added"] = {
                    "$gt": added_after_timestamp,
                }
        return parameters

    def process_filter(self, data, allowed, manifest_info):
        pipeline = [
            {"$match": {"$and": [self.full_query]}},
        ]

        # when no filter is provided only latest is considered.
        match_spec_version = self.filter_args.get("match[spec_version]")
        if not match_spec_version and "spec_version" in allowed:
            latest_pipeline = list(pipeline)
            latest_pipeline.append({"$sort": {"_manifest.media_type": pymongo.ASCENDING}})
            latest_pipeline.append({"$group": {"_id": "$id", "media_type": {"$last": "$_manifest.media_type"}}})

            query = [
                {"id": x["_id"], "_manifest.media_type": x["media_type"]}
                for x in list(data.aggregate(latest_pipeline))
            ]
            if query:
                pipeline.append({"$match": {"$or": query}})

        # create version filter
        if "version" in allowed:
            match_version = self.filter_args.get("match[version]")
            if not match_version:
                match_version = "last"
            if "all" not in match_version:
                actual_dates = [datetime_to_float(string_to_datetime(x)) for x in match_version.split(",") if (x != "first" and x != "last")]

                latest_pipeline = list(pipeline)
                latest_pipeline.append({"$sort": {"_manifest.version": pymongo.ASCENDING}})
                latest_pipeline.append({"$group": {"_id": "$id", "versions": {"$push": "$_manifest.version"}}})

                # The documents are sorted in ASCENDING order.
                version_selector = []
                if "last" in match_version:
                    version_selector.append({"$arrayElemAt": ["$versions", -1]})
                if "first" in match_version:
                    version_selector.append({"$arrayElemAt": ["$versions", 0]})
                for d in actual_dates:
                    version_selector.append({"$arrayElemAt": ["$versions", {"$indexOfArray": ["$versions", d]}]})
                latest_pipeline.append({"$addFields": {"versions": version_selector}})
                if actual_dates:
                    latest_pipeline.append({"$match": {"versions": {"$in": actual_dates}}})

                query = [
                    {"id": x["_id"], "_manifest.version": {"$in": x["versions"]}}
                    for x in list(data.aggregate(latest_pipeline))
                ]
                if query:
                    pipeline.append({"$match": {"$or": query}})

        pipeline.append({"$sort": {"_manifest.date_added": pymongo.ASCENDING, "created": pymongo.ASCENDING, "modified": pymongo.ASCENDING}})

        if manifest_info == "manifests":
            # Project the final results
            pipeline.append({"$project": {"_manifest": 1}})
            pipeline.append({"$replaceRoot": {"newRoot": "$_manifest"}})

            count = self.get_result_count(pipeline, data)
            self.add_pagination_operations(pipeline)
            results = list(data.aggregate(pipeline))
        elif manifest_info == "objects":
            # Project the final results
            pipeline.append({"$project": {"_id": 0, "_collection_id": 0, "_manifest": 0}})

            count = self.get_result_count(pipeline, data)
            self.add_pagination_operations(pipeline)
            results = list(data.aggregate(pipeline))
        else:
            # Return raw data from Mongodb
            count = self.get_result_count(pipeline, data)
            self.add_pagination_operations(pipeline)
            results = list(data.aggregate(pipeline))

        return count, results

    def add_pagination_operations(self, pipeline):
        if self.record:
            pipeline.append({"$skip": self.record["skip"]})
            pipeline.append({"$limit": self.record["limit"]})

    def get_result_count(self, pipeline, data):
        count_pipeline = list(pipeline)
        count_pipeline.append({"$count": "total"})
        count_result = list(data.aggregate(count_pipeline))

        if len(count_result) == 0:
            # No results
            return 0

        count = count_result[0]["total"]
        return count
