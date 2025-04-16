from data_agent.config_manager import ConfigManager


def test_get_and_set(config_manager):
    config_manager.set("app.interval", 45)
    assert config_manager.get("app.interval") == 45
    assert config_manager.app.interval == 45


def test_remove_key(config_manager):
    config_manager.set("app.non_removable", "keep")
    config_manager.set("app.removable", "to_delete")
    config_manager.remove("app.removable")
    assert config_manager.get("app.removable") is None
    assert config_manager.get("app.non_removable") == "keep"

    config_manager.set("app.folder.to_remove", "to_delete2")
    config_manager.set("app.folder.to_keep", "keep")
    assert config_manager.get("app.folder.to_remove") == "to_delete2"
    config_manager.remove("app.folder.to_remove")
    assert config_manager.get("app.folder.to_remove") is None
    assert config_manager.get("app.folder.to_keep") == "keep"


def test_thread_safety(config_manager):
    import threading

    def setter():
        for _ in range(500):
            config_manager.set("thread.test", "value", persist=False)

    threads = [threading.Thread(target=setter) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert config_manager.get("thread.test") == "value"


def test_persist_and_reload(temp_config_file):
    manager = ConfigManager(config_file=temp_config_file)
    manager.set("test.persist", "check")
    manager.persist()

    # Reload a new instance to verify persistence
    new_manager = ConfigManager(config_file=temp_config_file)
    assert new_manager.get("test.persist") == "check"


def test_dot_access(config_manager):
    config_manager.set("dot.access.test", "dotty")
    assert config_manager.dot.access.test == "dotty"
