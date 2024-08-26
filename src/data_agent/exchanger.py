from data_agent.exceptions import GroupAlreadyExists


class DataExchanger:
    def __init__(self, connection_manager):
        self._connection_manager = connection_manager

    def copy_period(
        self,
        src_conn,
        tags,
        dest_conn,
        dest_group,
        first_timestamp,
        last_timestamp,
        time_frequency=None,
        on_conflict="ask",
        progress_callback=None,
        batch_process=False,
    ):
        dest_group = dest_group.strip()

        if on_conflict == "ask":
            existing_groups = self._connection_manager.connection(
                dest_conn
            ).list_groups()

            if dest_group and dest_group in existing_groups:
                raise GroupAlreadyExists(f"{dest_group} already exist")

            else:
                # If dest group is empty - we copy each tag to it's own group
                for tag in tags:
                    if tag in existing_groups:
                        raise GroupAlreadyExists(f"{tag} already exist")

            on_conflict = "append"

        if batch_process:
            df = self._connection_manager.connection(src_conn).read_tag_values_period(
                tags=tags,
                first_timestamp=first_timestamp,
                last_timestamp=last_timestamp,
                time_frequency=time_frequency,
            )

            if dest_group:
                self._connection_manager.connection(
                    dest_conn
                ).write_group_values_period(dest_group, df, on_conflict=on_conflict)
            else:
                # If dest group is empty - we copy each tag to it's own group
                for i, tag in enumerate(tags):
                    if progress_callback:
                        progress_callback(tag, i + 1)

                    self._connection_manager.connection(
                        dest_conn
                    ).write_group_values_period(tag, df[tag], on_conflict=on_conflict)

        else:
            # Go one by one
            for i, tag in enumerate(tags):
                if progress_callback:
                    progress_callback(tag, i + 1)

                df = self._connection_manager.connection(
                    src_conn
                ).read_tag_values_period(
                    tags=[tag],
                    first_timestamp=first_timestamp,
                    last_timestamp=last_timestamp,
                    time_frequency=time_frequency,
                )

                if dest_group:
                    self._connection_manager.connection(
                        dest_conn
                    ).write_group_values_period(dest_group, df, on_conflict=on_conflict)
                else:
                    # If dest group is empty - we copy each tag to it's own group
                    self._connection_manager.connection(
                        dest_conn
                    ).write_group_values_period(
                        df.columns[0], df, on_conflict=on_conflict
                    )

    def copy_attributes(
        self,
        src_conn,
        tags,
        dest_conn,
        dest_group=None,
        attributes=None,
    ):
        dest_group = dest_group.strip()

        attr = self._connection_manager.connection(src_conn).read_tag_attributes(
            tags, attributes
        )

        if dest_group:
            attr = {
                f"{dest_group}{self._connection_manager.connection(dest_conn).GROUP_DELIMITER}{a}": attr[
                    a
                ]
                for a in attr
            }

        self._connection_manager.connection(dest_conn).write_tag_attributes(attr)
