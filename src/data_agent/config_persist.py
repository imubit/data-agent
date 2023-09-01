import confuse


class PersistConfig(confuse.Configuration):
    def add(self, obj):
        super(PersistConfig, self).add(obj)

        with open(self.user_config_path(), "w") as f:
            f.write(self.dump(full=False).strip())

    def set(self, value):
        super(PersistConfig, self).set(value)

        with open(self.user_config_path(), "w") as f:
            f.write(self.dump(full=False).strip())
