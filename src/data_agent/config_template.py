import confuse

CONFIG_SECTION_SERVICE = "service"
CONFIG_SECTION_BROKER = "broker"
CONFIG_SECTION_CONNECTION_MANAGER = "connections"
CONFIG_SECTION_SAFE_MANIPULATOR = "manipulated_tags"
CONFIG_SECTION_DAQ_SCHEDULER = "daq_jobs"
CONFIG_SECTION_LOG = "log"
CONFIG_SECTION_TRACE = "trace"

config_template = {
    # 'library': confuse.Filename(),
    # 'import_write': confuse.OneOf([bool, 'ask', 'skip']),
    # 'ignore': confuse.StrSeq(),
    # 'plugins': list,
    #
    # 'paths': {
    #     'directory': confuse.Filename(),
    #     'default': confuse.Filename(relative_to='directory'),
    # },
    CONFIG_SECTION_SERVICE: {
        "id": str,
        "type": str,
        "domain": str,
        "keep_alive_seconds": int,
    },
    CONFIG_SECTION_BROKER: {"uri": str, "timeout": int},  # seconds
    CONFIG_SECTION_CONNECTION_MANAGER: confuse.OneOf(
        [
            None,
            confuse.MappingTemplate(
                {"name": str, "type": str, "params": dict, "enabled": bool}
            ),
        ]
    ),
    CONFIG_SECTION_DAQ_SCHEDULER: confuse.OneOf(
        [
            None,
            confuse.MappingTemplate(
                {
                    "name": str,
                    "connection_name": str,
                    "tags": list,
                    "seconds": int,
                    "from_cache": bool,
                }
            ),
        ]
    ),
    CONFIG_SECTION_SAFE_MANIPULATOR: confuse.OneOf(
        [
            None,
            confuse.OneOf(
                [
                    None,
                    confuse.MappingTemplate(
                        {
                            "lower_bound": float,
                            "upper_bound": float,
                            "rate_bound": float,
                        }
                    ),
                ]
            ),
        ]
    ),
    CONFIG_SECTION_TRACE: {
        "slow_callbacks": float,
        "asyncio_debug_mode": bool,
        # 'hang_inspection': float,
    },
    CONFIG_SECTION_LOG: {
        "version": int,
        "disable_existing_loggers": bool,
        # 'formatters': dict,
        "formatters": {
            "standard": {"format": str, "datefmt": str},
            "brief": {"format": str, "datefmt": str},
        },
        "handlers": confuse.OneOf(
            [
                None,
                confuse.MappingTemplate(
                    {"level": str, "formatter": str, "class": str, "stream": str}
                ),
                confuse.MappingTemplate(
                    {
                        "level": str,
                        "formatter": str,
                        "class": str,
                        "filename": str,
                        "maxBytes": int,
                        "backupCount": int,
                    }
                ),
            ]
        ),
        # 'handlers': {
        #     'console': {
        #         'level': str,
        #         'formatter': str,
        #         'class': str,
        #         'stream': str
        #     },
        #     'file': {
        #         'level': str,
        #         'formatter': str,
        #         'class': str,
        #         'filename': str,
        #         'maxBytes': int,
        #         'backupCount': int,
        #     },
        #     'ntevent': confuse.Optional({
        #         'level': str,
        #         'formatter': str,
        #         'class': str,
        #         'stream': str
        #     })
        # },
        "loggers": {
            # 'asyncio': {
            #     'handlers': confuse.StrSeq(),
            #     'level': str,
            #     'propagate': bool
            # },
            # 'amqp_fabric.amq_broker_connector': {
            #     'handlers': confuse.StrSeq(),
            #     'level': str,
            #     'propagate': bool
            # },
            "apscheduler.scheduler": {
                "handlers": confuse.StrSeq(),
                "level": str,
                "propagate": bool,
            },
            "apscheduler.executors.default": {
                "handlers": confuse.StrSeq(),
                "level": str,
                "propagate": bool,
            },
            "apscheduler.executors.service_discovery": {
                "handlers": confuse.StrSeq(),
                "level": str,
                "propagate": bool,
            },
            # 'aio_pika': {
            #     'handlers': confuse.StrSeq(),
            #     'level': str,
            #     'propagate': bool
            # },
            # 'aio_pika.patterns.rpc': {
            #     'handlers': confuse.StrSeq(),
            #     'level': str,
            #     'propagate': bool
            # },
            # 'aiodebug.log_slow_callbacks': {
            #     'handlers': confuse.StrSeq(),
            #     'level': str,
            #     'propagate': bool
            # },
            "data_agent": {
                "handlers": confuse.StrSeq(),
                "level": str,
                "propagate": bool,
            },
            "__main__": {"handlers": confuse.StrSeq(), "level": str, "propagate": bool},
            "data_agent.main": {
                "handlers": confuse.StrSeq(),
                "level": str,
                "propagate": bool,
            },
            "data_agent.cli_main": {
                "handlers": confuse.StrSeq(),
                "level": str,
                "propagate": bool,
            },
            "data_agent.agent": {
                "handlers": confuse.StrSeq(),
                "level": str,
                "propagate": bool,
            },
            "data_agent.config_manager": {
                "handlers": confuse.StrSeq(),
                "level": str,
                "propagate": bool,
            },
            "data_agent.daq_scheduler": {
                "handlers": confuse.StrSeq(),
                "level": str,
                "propagate": bool,
            },
            "data_agent.connection_manager": {
                "handlers": confuse.StrSeq(),
                "level": str,
                "propagate": bool,
            },
            "data_agent.safe_manipulator": {
                "handlers": confuse.StrSeq(),
                "level": str,
                "propagate": bool,
            },
            "ia_plugin": {
                "handlers": confuse.StrSeq(),
                "level": str,
                "propagate": bool,
            },
        },
    },
}
