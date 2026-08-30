package luatest

import (
	_ "embed"
	"errors"

	"github.com/iceisfun/golua/compiler"
	"github.com/iceisfun/golua/parser"
	"github.com/iceisfun/golua/vm"
)

//go:embed sink_v1.lua
var sinkV1Source string

func (e *Engine) openSinkV1() error {
	block, err := parser.Parse("sink_v1.lua", sinkV1Source)
	if err != nil {
		return err
	}
	program, err := compiler.Compile("sink_v1.lua", block)
	if err != nil {
		return err
	}
	if _, err := e.luaVM.Run(program); err != nil {
		return err
	}
	sinkTable, err := requireGlobalTable(e.luaVM, "sink")
	if err != nil {
		return err
	}
	v1Table, err := requireTableField(sinkTable, "v1")
	if err != nil {
		return err
	}
	timeTable, err := requireTableField(v1Table, "time")
	if err != nil {
		return err
	}
	timeTable.SetString("now", vm.NewNativeFunc(e.timeNow))
	return nil
}

func (e *Engine) timeNow(state *vm.VM) int {
	if state.ArgCount() != 0 {
		panic("sink.v1.time.now expects no arguments")
	}
	if e.observedAt == "" {
		panic("sink.v1.time.now observation time is missing")
	}
	state.Set(0, vm.NewString(e.observedAt))
	return 1
}

func requireGlobalTable(luaVM *vm.VM, name string) (*vm.Table, error) {
	value := luaVM.GetGlobal(name)
	if !value.IsTable() {
		return nil, errors.New(name + " is not a table")
	}
	table, ok := value.AsTable().(*vm.Table)
	if !ok {
		return nil, errors.New(name + " is not a concrete table")
	}
	return table, nil
}

func requireTableField(parent *vm.Table, name string) (*vm.Table, error) {
	value := parent.GetString(name)
	if !value.IsTable() {
		return nil, errors.New(name + " is not a table")
	}
	table, ok := value.AsTable().(*vm.Table)
	if !ok {
		return nil, errors.New(name + " is not a concrete table")
	}
	return table, nil
}
