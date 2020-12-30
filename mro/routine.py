import mro.connection
import collections


class RoutineParameter:

    def __init__(self, name, data_type, mode):
        self.name = name
        self.data_type = data_type
        self.mode = mode

    def __repr__(self):
        return f"RoutineParameter({self.name}, {self.data_type}, {self.mode})"


class Routine:

    def __init__(self, name, in_parameters, out_parameters, return_type, routine_type):
        self.name = name
        self.in_parameters = in_parameters
        self.out_parameters = out_parameters
        self.return_type = return_type
        self.execute = 'call' if routine_type == 'PROCEDURE' else 'select * from'
        self.base_command = f"{self.execute} {self.name}({{}})"
        if return_type == 'void':
            self.return_function = self._return_void
        elif return_type == 'record':
            self.return_function = self._return_record
            self.out_type = collections.namedtuple(f"{name}_out", ' '.join([p.name for p in self.out_parameters]))
        else:
            self.return_function = self._return_scalar

    def __call__(self, *args, **kwargs):
        parameter_format = ', '.join(['%s' for x in args])
        if len(kwargs) > 0:
            parameter_format += ', ' + ', '.join([f"{k} := %s" for k in kwargs.keys()])
        parameters = args + tuple(kwargs.values())
        command = self.base_command.format(parameter_format)

        connection = mro.connection.connection
        cursor = connection.cursor()
        cursor.execute(command, parameters)
        connection.commit()
        return self.return_function(cursor)

    def _return_void(self, cursor):
        return None

    def _return_scalar(self, cursor):
        return next(cursor)[0]

    def _return_record(self, cursor):
        objs = []
        for row in cursor:
            objs.append(self.out_type(*row))
        return objs


def _create_routines(connection):
    cursor = connection.cursor()
    cursor.execute("select * from information_schema.routines where routine_type in ('PROCEDURE', 'FUNCTION') and routine_schema = 'public'")
    connection.commit()

    column_name_index_map = mro.helpers.create_column_name_index_map(cursor)
    for routine in cursor:
        routine_name = routine[column_name_index_map['routine_name']]
        routine_type = routine[column_name_index_map['routine_type']]
        specific_name = routine[column_name_index_map['specific_name']]

        cursor2 = connection.cursor()
        cursor2.execute(f"select * from information_schema.parameters where specific_name='{specific_name}' and parameter_mode <> 'OUT' order by ordinal_position;")
        connection.commit()

        in_parameters = []
        cim = mro.helpers.create_column_name_index_map(cursor2)
        for parameter in cursor2:
            in_parameters.append(RoutineParameter(parameter[cim['parameter_name']], parameter[cim['data_type']], parameter[cim['parameter_mode']]))

        # Using postgres specific tables because this information is not available in the information schema
        command = """
with r as (
    select proallargtypes, proargnames, proargmodes, prorettype from pg_proc where proname='{}'
), proallargtypes_expanded as (
    select a.index, a.t as oid from r, unnest(proallargtypes) with ordinality as a(t, index)
), proargnames_expanded as (
    select a.index, a.n as name from r, unnest(proargnames) with ordinality as a(n, index)
), proargmodes_expanded as (
    select a.index, a.m as mode from r, unnest(proargmodes) with ordinality as a(m, index)
), p as (
    select proallargtypes_expanded.index, oid, name, mode from proallargtypes_expanded join proargnames_expanded on proallargtypes_expanded.index = proargnames_expanded.index join proargmodes_expanded on proallargtypes_expanded.index = proargmodes_expanded.index
), params as (
    select p.index, p.oid, p.name, typname as data_type, p.mode from p join pg_type t on p.oid = t.oid
), outputs as (
    select index, oid, name, data_type, 'OUT' as mode from params where mode in ('o', 'b', 't')
)
select * from outputs order by index""".format(routine[column_name_index_map['routine_name']])
        cursor2 = connection.cursor()
        cursor2.execute(command)
        connection.commit()

        out_parameters = []
        cim = mro.helpers.create_column_name_index_map(cursor2)
        for parameter in cursor2:
            out_parameters.append(RoutineParameter(parameter[cim['name']], parameter[cim['data_type']], parameter[cim['mode']]))

        command = """
with r as (
    select prorettype from pg_proc where proname='{}'
)
select typname from pg_type t join r on t.oid = r.prorettype""".format(routine[column_name_index_map['routine_name']])
        cursor2 = connection.cursor()
        cursor2.execute(command)
        connection.commit()

        return_type = next(cursor2)[0]

        routine = Routine(routine_name, in_parameters, out_parameters, return_type, routine_type)
        setattr(mro, routine_name, routine)