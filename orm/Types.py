class Type:
    def __init__(self, name, converter, **kwargs):
        self.posgtre_name = name
        self.primary_key = kwargs.get("primary", False)
        self.non_null = kwargs.get("non_null", False)
        self.unique = kwargs.get("unique", False)
        self.default = kwargs.get("default")
        self.converter = converter

    def name_type(self):
        return self.posgtre_name

    def name_null(self):
        return " NOT NULL" if self.non_null else ""

    def name_primary_key(self):
        return " PRIMARY KEY" if self.primary_key else ""

    def name_unique(self):
        return " UNIQUE" if self.unique else ""

    def name_default(self):
        return " DEFAULT '"+self.default+"'" if self.default is not None else ""

    def initialize(self):
        return f"{self.name_type()}{self.name_primary_key()}{self.name_null()}{self.name_unique()}{self.name_default()}"

    def convert(self, *args, **kwargs):
        self.converter.convert(*args, **kwargs)


class Serial(Type):
    def __init__(self, **kwargs):
        kwargs["primary"] = True
        kwargs["non_null"] = True
        super().__init__("serial", **kwargs)


class Integer(Type):
    def __init__(self, **kwargs):
        super().__init__("integer", **kwargs)

        self.min_value = kwargs.get("min_value")
        self.max_value = kwargs.get("max_value")


class Text(Type):
    def __init__(self, **kwargs):
        super().__init__("text", **kwargs)